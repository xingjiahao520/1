import streamlit as st
import folium
from streamlit_folium import folium_static, st_folium
from folium import plugins
import random
import time
import math
import json
import os
import shutil
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
import pandas as pd
from dataclasses import dataclass, field

# ==================== 配置常量 ====================
@dataclass
class Config:
    """系统配置类"""
    SCHOOL_CENTER_GCJ: List[float] = field(default_factory=lambda: [118.7490, 32.2340])
    DEFAULT_A_GCJ: List[float] = field(default_factory=lambda: [118.749155 , 32.233767])
    DEFAULT_B_GCJ: List[float] = field(default_factory=lambda: [118.750110, 32.235460])
    CONFIG_FILE: str = "obstacle_config.json"
    BACKUP_DIR: str = "backups"
    DEFAULT_SAFETY_RADIUS_METERS: int = 5
    MAX_BACKUP_FILES: int = 10
    BASE_SPEED_MPS: float = 5.0
    HEARTBEAT_INTERVAL: float = 0.2
    VOLTAGE_VARIATION: float = 0.5
    SAT_RANGE: Tuple[int, int] = (8, 14)
    GAODE_SATELLITE_URL: str = "https://webst01.is.autonavi.com/appmaptile?style=6&x={x}&y={y}&z={z}"
    PATH_SAMPLE_POINTS: int = 40
    MAX_AVOID_ATTEMPTS: int = 12

config = Config()
os.makedirs(config.BACKUP_DIR, exist_ok=True)

# ==================== 坐标转换模块 ====================
class CoordinateConverter:
    """WGS-84 与 GCJ-02 坐标转换器"""
    a = 6378245.0
    ee = 0.00669342162296594323

    @classmethod
    def _transform_lat(cls, lng: float, lat: float) -> float:
        ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + 0.1 * lng * lat + 0.2 * math.sqrt(abs(lng))
        ret += (20.0 * math.sin(6.0 * lng * math.pi) + 20.0 * math.sin(2.0 * lng * math.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lat * math.pi) + 40.0 * math.sin(lat / 3.0 * math.pi)) * 2.0 / 3.0
        ret += (160.0 * math.sin(lat / 12.0 * math.pi) + 320 * math.sin(lat * math.pi / 30.0)) * 2.0 / 3.0
        return ret

    @classmethod
    def _transform_lng(cls, lng: float, lat: float) -> float:
        ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + 0.1 * lng * lat + 0.1 * math.sqrt(abs(lng))
        ret += (20.0 * math.sin(6.0 * lng * math.pi) + 20.0 * math.sin(2.0 * lng * math.pi)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lng * math.pi) + 40.0 * math.sin(lng / 3.0 * math.pi)) * 2.0 / 3.0
        ret += (150.0 * math.sin(lng / 12.0 * math.pi) + 300.0 * math.sin(lng / 30.0 * math.pi)) * 2.0 / 3.0
        return ret

    @classmethod
    def out_of_china(cls, lng: float, lat: float) -> bool:
        return not (72.004 <= lng <= 137.8347 and 0.8293 <= lat <= 55.8271)

    @classmethod
    def wgs84_to_gcj02(cls, lng: float, lat: float) -> Tuple[float, float]:
        if cls.out_of_china(lng, lat):
            return lng, lat
        dlat = cls._transform_lat(lng - 105.0, lat - 35.0)
        dlng = cls._transform_lng(lng - 105.0, lat - 35.0)
        radlat = lat / 180.0 * math.pi
        magic = math.sin(radlat)
        magic = 1 - cls.ee * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((cls.a * (1 - cls.ee)) / (magic * sqrtmagic) * math.pi)
        dlng = (dlng * 180.0) / (cls.a / sqrtmagic * math.cos(radlat) * math.pi)
        return lng + dlng, lat + dlat

    @classmethod
    def gcj02_to_wgs84(cls, lng: float, lat: float) -> Tuple[float, float]:
        if cls.out_of_china(lng, lat):
            return lng, lat
        dlat = cls._transform_lat(lng - 105.0, lat - 35.0)
        dlng = cls._transform_lng(lng - 105.0, lat - 35.0)
        radlat = lat / 180.0 * math.pi
        magic = math.sin(radlat)
        magic = 1 - cls.ee * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((cls.a * (1 - cls.ee)) / (magic * sqrtmagic) * math.pi)
        dlng = (dlng * 180.0) / (cls.a / sqrtmagic * math.cos(radlat) * math.pi)
        return lng * 2 - (lng + dlng), lat * 2 - (lat + dlat)

    @classmethod
    def convert_batch(cls, coords: List[Tuple[float, float]], direction: str = "wgs84_to_gcj02") -> List[Tuple[float, float]]:
        converter = cls.wgs84_to_gcj02 if direction == "wgs84_to_gcj02" else cls.gcj02_to_wgs84
        return [converter(lng, lat) for lng, lat in coords]

    @classmethod
    def calculate_offset(cls, lng: float, lat: float) -> Tuple[float, float]:
        gcj_lng, gcj_lat = cls.wgs84_to_gcj02(lng, lat)
        return gcj_lng - lng, gcj_lat - lat

# ==================== 通信链路模拟器 ====================
@dataclass
class CommunicationLog:
    timestamp: str
    direction: str
    message: str
    details: str = ""

class CommunicationSimulator:
    def __init__(self):
        self.gcs_ip = "192.168.1.100"
        self.obc_ip = "192.168.1.101"
        self.fcu_ip = "192.168.1.102"
        self.gcs_online = True
        self.obc_online = True
        self.fcu_online = True
        self.gcs_obc_latency = 25
        self.obc_fcu_latency = 15
        self.packet_loss_rate = 0.001
        self.logs: List[CommunicationLog] = []
        self.total_packets_sent = 0
        self.total_packets_received = 0
        self.total_packets_lost = 0
        self.planning_records: List[Dict] = []

    def send_message(self, src: str, dst: str, message: str, details: str = "") -> bool:
        self.total_packets_sent += 1
        if not self.check_link_status(src, dst):
            self.total_packets_lost += 1
            return False
        if random.random() < self.packet_loss_rate:
            self.total_packets_lost += 1
            return False
        delay = self.get_link_delay(src, dst)
        time.sleep(delay / 1000)
        self.total_packets_received += 1
        log = CommunicationLog(datetime.now().strftime("%H:%M:%S"), f"{src}→{dst}", message, details)
        self.logs.insert(0, log)
        if len(self.logs) > 100:
            self.logs.pop()
        return True

    def send_relayed_message(self, src: str, relay: str, dst: str, message: str, details: str = "") -> bool:
        return self.send_message(src, relay, message, details) and self.send_message(relay, dst, message, details)

    def check_link_status(self, src: str, dst: str) -> bool:
        if src == "GCS" and dst == "OBC":
            return self.gcs_online and self.obc_online
        elif src == "OBC" and dst == "GCS":
            return self.obc_online and self.gcs_online
        elif src == "OBC" and dst == "FCU":
            return self.obc_online and self.fcu_online
        elif src == "FCU" and dst == "OBC":
            return self.fcu_online and self.obc_online
        return False

    def get_link_delay(self, src: str, dst: str) -> float:
        if (src == "GCS" and dst == "OBC") or (src == "OBC" and dst == "GCS"):
            return self.gcs_obc_latency
        elif (src == "OBC" and dst == "FCU") or (src == "FCU" and dst == "OBC"):
            return self.obc_fcu_latency
        return 10

    def get_statistics(self) -> Dict:
        success_rate = (self.total_packets_received / self.total_packets_sent * 100) if self.total_packets_sent > 0 else 0
        return {
            "sent": self.total_packets_sent,
            "received": self.total_packets_received,
            "lost": self.total_packets_lost,
            "success_rate": success_rate,
            "gcs_obc_latency": self.gcs_obc_latency,
            "obc_fcu_latency": self.obc_fcu_latency,
            "packet_loss_rate": self.packet_loss_rate
        }

    def reset_statistics(self):
        self.total_packets_sent = self.total_packets_received = self.total_packets_lost = 0
        self.logs.clear()
        self.planning_records.clear()

    def add_planning_record(self, record: Dict):
        record["timestamp"] = datetime.now().strftime("%H:%M:%S")
        self.planning_records.insert(0, record)
        if len(self.planning_records) > 20:
            self.planning_records.pop()

# ==================== 几何函数 ====================
def point_in_polygon(point: List[float], polygon: List[List[float]]) -> bool:
    x, y = point
    inside = False
    n = len(polygon)
    for i in range(n):
        x1, y1 = polygon[i]
        x2, y2 = polygon[(i + 1) % n]
        if ((y1 > y) != (y2 > y)) and (x < (x2 - x1) * (y - y1) / (y2 - y1) + x1):
            inside = not inside
    return inside

def on_segment(p: List[float], q: List[float], r: List[float]) -> bool:
    return (min(p[0], r[0]) <= q[0] <= max(p[0], r[0]) and
            min(p[1], r[1]) <= q[1] <= max(p[1], r[1]))

def orientation(p: List[float], q: List[float], r: List[float]) -> int:
    val = (q[1] - p[1]) * (r[0] - q[0]) - (q[0] - p[0]) * (r[1] - q[1])
    if abs(val) < 1e-10:
        return 0
    return 1 if val > 0 else 2

def segments_intersect(p1: List[float], p2: List[float], p3: List[float], p4: List[float]) -> bool:
    o1 = orientation(p1, p2, p3)
    o2 = orientation(p1, p2, p4)
    o3 = orientation(p3, p4, p1)
    o4 = orientation(p3, p4, p2)

    if o1 != o2 and o3 != o4:
        return True
    if o1 == 0 and on_segment(p1, p3, p2):
        return True
    if o2 == 0 and on_segment(p1, p4, p2):
        return True
    if o3 == 0 and on_segment(p3, p1, p4):
        return True
    if o4 == 0 and on_segment(p3, p2, p4):
        return True
    return False

def line_intersects_polygon(p1: List[float], p2: List[float], polygon: List[List[float]]) -> bool:
    if point_in_polygon(p1, polygon) or point_in_polygon(p2, polygon):
        return True
    n = len(polygon)
    for i in range(n):
        p3 = polygon[i]
        p4 = polygon[(i + 1) % n]
        if segments_intersect(p1, p2, p3, p4):
            return True
    return False

def distance(p1: List[float], p2: List[float]) -> float:
    return math.hypot(p1[0] - p2[0], p1[1] - p2[1])

def get_polygon_bounds(polygon: List[List[float]]) -> Optional[Dict]:
    if not polygon:
        return None
    lngs = [p[0] for p in polygon]
    lats = [p[1] for p in polygon]
    return {
        'min_lng': min(lngs), 'max_lng': max(lngs),
        'min_lat': min(lats), 'max_lat': max(lats),
        'center_lng': (min(lngs) + max(lngs)) / 2,
        'center_lat': (min(lats) + max(lats)) / 2
    }

def validate_polygon(polygon: List[List[float]]) -> bool:
    return len(polygon) >= 3

def point_to_segment_distance_deg(point: List[float], seg_start: List[float], seg_end: List[float]) -> float:
    px, py = point
    x1, y1 = seg_start
    x2, y2 = seg_end
    dx = x2 - x1
    dy = y2 - y1
    len_sq = dx * dx + dy * dy
    if len_sq == 0:
        return math.hypot(px - x1, py - y1)
    t = ((px - x1) * dx + (py - y1) * dy) / len_sq
    t = max(0, min(1, t))
    proj_x = x1 + t * dx
    proj_y = y1 + t * dy
    return math.hypot(px - proj_x, py - proj_y)

def point_to_segment_distance_meters(point: List[float], seg_start: List[float], seg_end: List[float]) -> float:
    return point_to_segment_distance_deg(point, seg_start, seg_end) * 111000

def check_safety_radius(drone_pos: List[float], obstacles_gcj: List[Dict], flight_altitude: float, safety_radius: float) -> Tuple[bool, Optional[float], Optional[str]]:
    if not drone_pos:
        return True, None, None
    min_distance = float('inf')
    danger_name = None
    for obs in obstacles_gcj:
        coords = obs.get('polygon', [])
        obs_height = obs.get('height', 30)
        if obs_height <= flight_altitude:
            continue
        if coords and len(coords) >= 3:
            for i in range(len(coords)):
                p1 = coords[i]
                p2 = coords[(i + 1) % len(coords)]
                dist_m = point_to_segment_distance_meters(drone_pos, p1, p2)
                if dist_m < min_distance:
                    min_distance = dist_m
                    danger_name = obs.get('name', '障碍物')
    if min_distance < safety_radius:
        return False, min_distance, danger_name
    return True, min_distance if min_distance != float('inf') else None, None

# ==================== 障碍物管理 ====================
def cleanup_old_backups():
    try:
        backup_files = [f for f in os.listdir(config.BACKUP_DIR) if f.startswith(config.CONFIG_FILE)]
        if len(backup_files) > config.MAX_BACKUP_FILES:
            backup_files.sort()
            for old_file in backup_files[:-config.MAX_BACKUP_FILES]:
                os.remove(os.path.join(config.BACKUP_DIR, old_file))
    except Exception as e:
        st.warning(f"清理备份文件时出错: {e}")

def backup_config() -> Optional[str]:
    if os.path.exists(config.CONFIG_FILE):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f"{config.BACKUP_DIR}/{config.CONFIG_FILE}.{timestamp}.bak"
        try:
            shutil.copy(config.CONFIG_FILE, backup_name)
            cleanup_old_backups()
            return backup_name
        except Exception as e:
            st.error(f"备份失败: {e}")
            return None
    return None

def load_obstacles() -> List[Dict]:
    if os.path.exists(config.CONFIG_FILE):
        try:
            with open(config.CONFIG_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                obstacles = data.get('obstacles', [])
                for obs in obstacles:
                    if 'selected' not in obs:
                        obs['selected'] = False
                    if 'height' not in obs:
                        obs['height'] = 30
                return obstacles
        except (json.JSONDecodeError, IOError) as e:
            st.error(f"加载配置文件失败: {e}")
            return []
    return []

def save_obstacles(obstacles: List[Dict]) -> bool:
    try:
        backup_config()
        data = {
            'obstacles': obstacles,
            'count': len(obstacles),
            'save_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'version': 'v13.2'
        }
        with open(config.CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        st.error(f"保存失败: {e}")
        return False

def get_latest_backup() -> Optional[str]:
    try:
        backup_files = [f for f in os.listdir(config.BACKUP_DIR) if f.startswith(config.CONFIG_FILE) and f.endswith('.bak')]
        if backup_files:
            backup_files.sort(reverse=True)
            return os.path.join(config.BACKUP_DIR, backup_files[0])
    except Exception as e:
        st.error(f"获取备份文件失败: {e}")
    return None

def restore_from_backup(backup_path: str) -> bool:
    try:
        with open(backup_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            obstacles = data.get('obstacles', [])
            save_obstacles(obstacles)
            return True
    except Exception as e:
        st.error(f"恢复备份失败: {e}")
        return False

# ==================== 优化的绕行算法 ====================
import math
import random
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass

@dataclass
class ObstacleInfo:
    """障碍物信息"""
    polygon: List[List[float]]
    name: str
    height: float
    center: List[float]
    bounding_box: Tuple[float, float, float, float]
    min_lat: float
    max_lat: float
    min_lng: float
    max_lng: float

def get_blocking_obstacles(start: List[float], end: List[float], obstacles_gcj: List[Dict], flight_altitude: float) -> List[Dict]:
    """获取阻挡航线的障碍物"""
    blocking = []
    for obs in obstacles_gcj:
        if obs.get('height', 30) > flight_altitude:
            coords = obs.get('polygon', [])
            if coords and line_intersects_polygon(start, end, coords):
                blocking.append(obs)
    return blocking

def get_obstacle_extent(obstacles: List[Dict]) -> Tuple[float, float, float, float]:
    """获取障碍物群边界"""
    min_lng, max_lng = float('inf'), -float('inf')
    min_lat, max_lat = float('inf'), -float('inf')
    for obs in obstacles:
        for point in obs.get('polygon', []):
            min_lng = min(min_lng, point[0])
            max_lng = max(max_lng, point[0])
            min_lat = min(min_lat, point[1])
            max_lat = max(max_lat, point[1])
    return min_lng, max_lng, min_lat, max_lat

def get_obstacle_center(obstacle: Dict) -> List[float]:
    """获取障碍物中心点"""
    poly = obstacle.get('polygon', [])
    if not poly:
        return [0, 0]
    lngs = [p[0] for p in poly]
    lats = [p[1] for p in poly]
    return [(min(lngs) + max(lngs)) / 2, (min(lats) + max(lats)) / 2]

def get_obstacle_bounds(obstacle: Dict) -> Tuple[float, float, float, float]:
    """获取障碍物边界"""
    poly = obstacle.get('polygon', [])
    if not poly:
        return 0, 0, 0, 0
    lngs = [p[0] for p in poly]
    lats = [p[1] for p in poly]
    return min(lngs), max(lngs), min(lats), max(lats)

def get_obstacle_info(obstacle: Dict) -> ObstacleInfo:
    """获取障碍物详细信息"""
    min_lng, max_lng, min_lat, max_lat = get_obstacle_bounds(obstacle)
    return ObstacleInfo(
        polygon=obstacle.get('polygon', []),
        name=obstacle.get('name', '障碍物'),
        height=obstacle.get('height', 30),
        center=[(min_lng + max_lng) / 2, (min_lat + max_lat) / 2],
        bounding_box=(min_lng, max_lng, min_lat, max_lat),
        min_lat=min_lat,
        max_lat=max_lat,
        min_lng=min_lng,
        max_lng=max_lng
    )

def is_point_safe(point: List[float], obstacles: List[Dict], flight_altitude: float, safety_radius: float) -> bool:
    """检查点是否安全"""
    for obs in obstacles:
        if obs.get('height', 30) <= flight_altitude:
            continue
        poly = obs.get('polygon', [])
        if not poly:
            continue
        if point_in_polygon(point, poly):
            return False
        for i in range(len(poly)):
            p1 = poly[i]
            p2 = poly[(i + 1) % len(poly)]
            dist_m = point_to_segment_distance_meters(point, p1, p2)
            if dist_m < safety_radius:
                return False
    return True

def is_path_segment_clear(p1: List[float], p2: List[float], obstacles: List[Dict], 
                          flight_altitude: float, safety_radius: float) -> bool:
    """检查线段是否安全"""
    for obs in obstacles:
        if obs.get('height', 30) <= flight_altitude:
            continue
        poly = obs.get('polygon', [])
        if not poly:
            continue
        
        if line_intersects_polygon(p1, p2, poly):
            return False
        
        # 采样检查
        sample_count = max(20, int(distance(p1, p2) * 111000 / 3))
        for k in range(sample_count + 1):
            t = k / sample_count
            px = p1[0] + (p2[0] - p1[0]) * t
            py = p1[1] + (p2[1] - p1[1]) * t
            point = [px, py]
            
            if point_in_polygon(point, poly):
                return False
            
            for i in range(len(poly)):
                p3 = poly[i]
                p4 = poly[(i + 1) % len(poly)]
                dist_m = point_to_segment_distance_meters(point, p3, p4)
                if dist_m < safety_radius:
                    return False
    return True

def find_closest_safe_point_on_line(p1: List[float], p2: List[float], 
                                     obstacles: List[Dict], flight_altitude: float, 
                                     safety_radius: float, target_side: str) -> Optional[List[float]]:
    """在线段上找到最接近障碍物但安全的点"""
    best_point = None
    best_distance = float('inf')
    
    # 采样线段上的点
    sample_count = 50
    for k in range(sample_count + 1):
        t = k / sample_count
        px = p1[0] + (p2[0] - p1[0]) * t
        py = p1[1] + (p2[1] - p1[1]) * t
        point = [px, py]
        
        # 检查该点是否安全
        safe = True
        min_dist_to_obs = float('inf')
        
        for obs in obstacles:
            if obs.get('height', 30) <= flight_altitude:
                continue
            poly = obs.get('polygon', [])
            if not poly:
                continue
            
            if point_in_polygon(point, poly):
                safe = False
                break
            
            for i in range(len(poly)):
                p3 = poly[i]
                p4 = poly[(i + 1) % len(poly)]
                dist_m = point_to_segment_distance_meters(point, p3, p4)
                min_dist_to_obs = min(min_dist_to_obs, dist_m)
                if dist_m < safety_radius:
                    safe = False
                    break
            if not safe:
                break
        
        if safe:
            # 优先选择距离障碍物接近安全半径的点
            score = abs(min_dist_to_obs - safety_radius) if min_dist_to_obs != float('inf') else float('inf')
            if score < best_distance:
                best_distance = score
                best_point = point
    
    return best_point

def generate_adaptive_waypoints(start: List[float], end: List[float],
                                 obstacles: List[Dict], flight_altitude: float,
                                 safety_radius: float, side: str) -> List[List[float]]:
    """生成自适应绕行航点 - 核心算法"""
    
    if not obstacles:
        return [start, end]
    
    # 获取所有障碍物的详细信息
    obstacle_infos = [get_obstacle_info(obs) for obs in obstacles]
    
    # 计算整体边界
    min_lng = min(obs.min_lng for obs in obstacle_infos)
    max_lng = max(obs.max_lng for obs in obstacle_infos)
    min_lat = min(obs.min_lat for obs in obstacle_infos)
    max_lat = max(obs.max_lat for obs in obstacle_infos)
    
    # 计算经纬度转换系数
    mid_lat = (start[1] + end[1]) / 2
    deg_per_meter_lng = 1 / (111000 * math.cos(math.radians(mid_lat)))
    deg_per_meter_lat = 1 / 111000
    
    # 安全偏移距离（紧贴障碍物）
    safe_offset_m = safety_radius + 0.5
    safe_offset_lng = safe_offset_m * deg_per_meter_lng
    safe_offset_lat = safe_offset_m * deg_per_meter_lat
    
    # 确定绕行侧边界
    if side == "right":
        boundary_lng = max_lng + safe_offset_lng
        side_name = "right"
    else:
        boundary_lng = min_lng - safe_offset_lng
        side_name = "left"
    
    # 构建自适应航点纬度列表
    waypoint_lats = []
    
    # 添加起点附近航点
    if start[1] < min_lat:
        waypoint_lats.append(start[1])
        waypoint_lats.append(start[1] + (min_lat - start[1]) * 0.3)
        waypoint_lats.append(start[1] + (min_lat - start[1]) * 0.6)
    
    # 为每个障碍物添加上下边界航点
    all_boundary_lats = []
    for obs in obstacle_infos:
        all_boundary_lats.append(obs.min_lat - safe_offset_lat * 0.5)
        all_boundary_lats.append(obs.min_lat)
        all_boundary_lats.append(obs.max_lat)
        all_boundary_lats.append(obs.max_lat + safe_offset_lat * 0.5)
        
        # 添加障碍物中心附近航点
        all_boundary_lats.append(obs.center[1])
        all_boundary_lats.append(obs.center[1] - safe_offset_lat * 0.3)
        all_boundary_lats.append(obs.center[1] + safe_offset_lat * 0.3)
    
    waypoint_lats.extend(sorted(set(all_boundary_lats)))
    
    # 添加终点附近航点
    if end[1] > max_lat:
        waypoint_lats.append(end[1] - (end[1] - max_lat) * 0.4)
        waypoint_lats.append(end[1] - (end[1] - max_lat) * 0.7)
        waypoint_lats.append(end[1])
    else:
        waypoint_lats.append(end[1])
    
    # 去重并排序
    waypoint_lats = sorted(set(waypoint_lats))
    
    # 过滤掉不在起点和终点之间的纬度
    min_valid_lat = min(start[1], end[1]) - safe_offset_lat
    max_valid_lat = max(start[1], end[1]) + safe_offset_lat
    waypoint_lats = [lat for lat in waypoint_lats if min_valid_lat <= lat <= max_valid_lat]
    
    # 确保有足够的航点
    if len(waypoint_lats) < 5:
        # 添加中间航点
        for i in range(1, 6):
            t = i / 6
            lat = start[1] + (end[1] - start[1]) * t
            waypoint_lats.append(lat)
        waypoint_lats = sorted(set(waypoint_lats))
    
    # 生成绕行路径
    best_path = None
    best_score = float('inf')
    
    # 尝试不同的偏移因子（从紧贴到稍远）
    for factor_idx, factor in enumerate([0.8, 1.0, 1.2, 1.5, 2.0]):
        current_offset_m = safe_offset_m * factor
        current_offset_lng = current_offset_m * deg_per_meter_lng
        
        waypoints = []
        
        for lat in waypoint_lats:
            # 基础经度
            waypoint_lng = boundary_lng
            
            # 根据纬度位置动态调整（靠近障碍物中心时稍微外扩）
            adjustment = 0
            for obs in obstacle_infos:
                if obs.min_lat - safe_offset_lat <= lat <= obs.max_lat + safe_offset_lat:
                    # 计算距离障碍物中心的距离
                    dist_to_center = abs(lat - obs.center[1])
                    if dist_to_center < safe_offset_lat * 2:
                        # 靠近障碍物中心时增加偏移
                        extra = (1 - dist_to_center / (safe_offset_lat * 2)) * 0.3
                        adjustment = max(adjustment, extra)
            
            # 最终经度
            final_lng = boundary_lng + (current_offset_lng * (1 + adjustment)) if side == "right" else boundary_lng - (current_offset_lng * (1 + adjustment))
            waypoints.append([final_lng, lat])
        
        # 构建完整路径
        candidate = [start] + waypoints + [end]
        
        # 验证路径安全性
        is_valid = True
        for i in range(len(candidate) - 1):
            if not is_path_segment_clear(candidate[i], candidate[i+1], obstacles, flight_altitude, safety_radius):
                is_valid = False
                break
        
        if is_valid:
            # 计算路径评分（长度优先，偏移量小优先）
            path_len = sum(distance(candidate[i], candidate[i+1]) for i in range(len(candidate)-1))
            score = path_len * 111000 + factor_idx * 10
            
            if score < best_score:
                best_score = score
                best_path = candidate
                break  # 找到第一条有效路径就使用
    
    # 如果没有找到有效路径，使用保底方案
    if not best_path:
        # 大幅增加偏移量
        large_offset_m = safe_offset_m * 3
        large_offset_lng = large_offset_m * deg_per_meter_lng
        
        waypoints = []
        simplified_lats = []
        # 简化航点数量
        step = max(1, len(waypoint_lats) // 8)
        for i in range(0, len(waypoint_lats), step):
            lat = waypoint_lats[i]
            waypoint_lng = boundary_lng + large_offset_lng if side == "right" else boundary_lng - large_offset_lng
            waypoints.append([waypoint_lng, lat])
            simplified_lats.append(lat)
        
        # 确保起点和终点在路径中
        if start[1] not in simplified_lats:
            waypoints.insert(0, [start[0] + (0.0001 if side == "right" else -0.0001), start[1]])
        if end[1] not in simplified_lats:
            waypoints.append([end[0] + (0.0001 if side == "right" else -0.0001), end[1]])
        
        best_path = [start] + waypoints + [end]
    
    # 路径优化：去除不必要的航点
    optimized = [best_path[0]]
    i = 0
    while i < len(best_path) - 1:
        # 尝试跳过中间点
        furthest = i + 1
        for j in range(i + 2, len(best_path)):
            if is_path_segment_clear(best_path[i], best_path[j], obstacles, flight_altitude, safety_radius):
                furthest = j
            else:
                break
        optimized.append(best_path[furthest])
        i = furthest
    
    return optimized

def find_left_avoidance_path(start: List[float], end: List[float], obstacles_gcj: List[Dict],
                              flight_altitude: float, safety_radius: float = 5) -> List[List[float]]:
    """向左绕行路径"""
    blocking = get_blocking_obstacles(start, end, obstacles_gcj, flight_altitude)
    if not blocking:
        return [start, end]
    return generate_adaptive_waypoints(start, end, blocking, flight_altitude, safety_radius, "left")

def find_right_avoidance_path(start: List[float], end: List[float], obstacles_gcj: List[Dict],
                               flight_altitude: float, safety_radius: float = 5) -> List[List[float]]:
    """向右绕行路径"""
    blocking = get_blocking_obstacles(start, end, obstacles_gcj, flight_altitude)
    if not blocking:
        return [start, end]
    return generate_adaptive_waypoints(start, end, blocking, flight_altitude, safety_radius, "right")

def find_best_avoidance_path(start: List[float], end: List[float], obstacles_gcj: List[Dict],
                              flight_altitude: float, safety_radius: float = 5) -> List[List[float]]:
    """选择最佳绕行路径"""
    # 检查直线是否安全
    if is_path_segment_clear(start, end, obstacles_gcj, flight_altitude, safety_radius):
        return [start, end]
    
    # 计算左右路径
    left_path = find_left_avoidance_path(start, end, obstacles_gcj, flight_altitude, safety_radius)
    right_path = find_right_avoidance_path(start, end, obstacles_gcj, flight_altitude, safety_radius)
    
    # 计算路径长度
    left_len = sum(distance(left_path[i], left_path[i+1]) for i in range(len(left_path)-1))
    right_len = sum(distance(right_path[i], right_path[i+1]) for i in range(len(right_path)-1))
    
    return left_path if left_len <= right_len else right_path

def create_avoidance_path(start: List[float], end: List[float], obstacles_gcj: List[Dict],
                          flight_altitude: float, direction: str, safety_radius: float = 5) -> Optional[List[List[float]]]:
    """创建绕行路径的主入口函数"""
    if not start or not end:
        return None
    
    # 检查直线是否安全
    straight_safe = True
    for obs in obstacles_gcj:
        if obs.get('height', 30) > flight_altitude:
            coords = obs.get('polygon', [])
            if coords and line_intersects_polygon(start, end, coords):
                straight_safe = False
                break
    
    if straight_safe:
        return [start, end]
    
    # 根据方向生成绕行路径
    if direction == "向左绕行":
        result = find_left_avoidance_path(start, end, obstacles_gcj, flight_altitude, safety_radius)
    elif direction == "向右绕行":
        result = find_right_avoidance_path(start, end, obstacles_gcj, flight_altitude, safety_radius)
    else:  # 最佳航线
        result = find_best_avoidance_path(start, end, obstacles_gcj, flight_altitude, safety_radius)
    
    if not result or len(result) < 2:
        return [start, end]
    
    return result

def calculate_path_length(path: List[List[float]]) -> float:
    """计算路径长度（度）"""
    if not path or len(path) < 2:
        return 0.0
    return sum(distance(path[i], path[i + 1]) for i in range(len(path) - 1))

# ==================== 心跳包模拟器 ====================
@dataclass
class HeartbeatData:
    timestamp: str
    flight_time: float
    lat: float
    lng: float
    altitude: float
    voltage: float
    satellites: int
    speed: float
    progress: float
    arrived: bool
    safety_violation: bool
    remaining_distance: float

class HeartbeatSimulator:
    def __init__(self, start_point_gcj: List[float]):
        self.history: List[HeartbeatData] = []
        self.current_pos: List[float] = start_point_gcj.copy()
        self.path: List[List[float]] = [start_point_gcj.copy()]
        self.path_index: int = 0
        self.simulating: bool = False
        self.flight_altitude: float = 50
        self.speed: int = 50
        self.progress: float = 0.0
        self.total_distance: float = 0.0
        self.distance_traveled: float = 0.0
        self.safety_radius: float = config.DEFAULT_SAFETY_RADIUS_METERS
        self.safety_violation: bool = False
        self.start_time: Optional[datetime] = None
        self.flight_log: List[HeartbeatData] = []
        self.last_update_time: Optional[float] = None

    def set_path(self, path: List[List[float]], altitude: float = 50, speed: int = 50, safety_radius: float = 5):
        if not path or len(path) < 2:
            return
        self.path = path
        self.path_index = 0
        self.current_pos = path[0].copy()
        self.flight_altitude = altitude
        self.speed = speed
        self.safety_radius = safety_radius
        self.simulating = True
        self.progress = 0.0
        self.distance_traveled = 0.0
        self.safety_violation = False
        self.start_time = datetime.now()
        self.last_update_time = None
        self.total_distance = sum(distance(path[i], path[i + 1]) for i in range(len(path) - 1))

    def update_and_generate(self, obstacles_gcj: List[Dict], comm_sim: Optional[Any] = None) -> Optional[HeartbeatData]:
        if not self.simulating or self.path_index >= len(self.path) - 1:
            if self.simulating:
                self.simulating = False
                if comm_sim:
                    comm_sim.send_relayed_message("FCU", "OBC", "GCS", "MISSION_COMPLETE", "任务完成")
            return None

        current_time = time.time()
        if self.last_update_time is None:
            delta_time = config.HEARTBEAT_INTERVAL
        else:
            delta_time = min(0.5, current_time - self.last_update_time)
        self.last_update_time = current_time

        start = self.path[self.path_index]
        end = self.path[self.path_index + 1]
        segment_distance = distance(start, end)
        
        if segment_distance < 1e-9:
            self.path_index += 1
            self.distance_traveled = 0
            if self.path_index >= len(self.path) - 1:
                self.simulating = False
                return self._generate_heartbeat(True)
            return self._generate_heartbeat(False)

        speed_m_per_s = config.BASE_SPEED_MPS * (self.speed / 100)
        move_distance = speed_m_per_s * delta_time
        old_path_index = self.path_index
        self.distance_traveled += move_distance

        if self.distance_traveled < 0:
            self.distance_traveled = 0

        if self.total_distance > 0:
            completed_distance = 0.0
            for i in range(self.path_index):
                completed_distance += distance(self.path[i], self.path[i + 1])
            segment_progress = min(1.0, max(0.0, self.distance_traveled / segment_distance))
            completed_distance += segment_distance * segment_progress
            self.progress = min(1.0, completed_distance / self.total_distance)

        if self.distance_traveled >= segment_distance - 1e-9 and self.distance_traveled > 0:
            if comm_sim and old_path_index < len(self.path) - 1:
                waypoint_num = old_path_index + 1
                total_waypoints = len(self.path) - 1
                comm_sim.send_message("FCU", "OBC", f"WP_REACHED #{waypoint_num}", f"到达航点 {waypoint_num}/{total_waypoints}")
                comm_sim.send_relayed_message("FCU", "OBC", "GCS", f"WP_REACHED #{waypoint_num}", f"航点 {waypoint_num} 已到达")
            self.path_index += 1
            self.distance_traveled = 0
            if self.path_index < len(self.path):
                self.current_pos = self.path[self.path_index].copy()
            else:
                self.simulating = False
                if comm_sim:
                    comm_sim.send_relayed_message("FCU", "OBC", "GCS", "MISSION_COMPLETE", "所有航点已完成")
                return self._generate_heartbeat(True)
        elif segment_distance > 0:
            t = min(1.0, max(0.0, self.distance_traveled / segment_distance))
            lng = start[0] + (end[0] - start[0]) * t
            lat = start[1] + (end[1] - start[1]) * t
            self.current_pos = [lng, lat]

        safe, _, _ = check_safety_radius(self.current_pos, obstacles_gcj, self.flight_altitude, self.safety_radius)
        if not safe and not self.safety_violation:
            self.safety_violation = True
            if comm_sim:
                comm_sim.send_relayed_message("FCU", "OBC", "GCS", "SAFETY_VIOLATION", "警告：进入危险区域")

        return self._generate_heartbeat(False)

    def _generate_heartbeat(self, arrived: bool = False) -> HeartbeatData:
        flight_time = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0

        if arrived:
            remaining_dist = 0.0
        else:
            remaining_in_path = 0.0
            if self.path_index < len(self.path) - 1:
                segment_remaining = distance(self.current_pos, self.path[self.path_index + 1])
                remaining_in_path += max(0, segment_remaining)
                for i in range(self.path_index + 1, len(self.path) - 1):
                    remaining_in_path += distance(self.path[i], self.path[i + 1])
            remaining_dist = remaining_in_path * 111000

        heartbeat = HeartbeatData(
            timestamp=datetime.now().strftime("%H:%M:%S"),
            flight_time=flight_time,
            lat=self.current_pos[1],
            lng=self.current_pos[0],
            altitude=self.flight_altitude,
            voltage=round(22.2 + random.uniform(-config.VOLTAGE_VARIATION, config.VOLTAGE_VARIATION), 1),
            satellites=random.randint(*config.SAT_RANGE),
            speed=round(config.BASE_SPEED_MPS * (self.speed / 100), 1),
            progress=self.progress,
            arrived=arrived,
            safety_violation=self.safety_violation,
            remaining_distance=remaining_dist
        )
        self.history.insert(0, heartbeat)
        if len(self.history) > 100:
            self.history.pop()
        self.flight_log.append(heartbeat)
        if len(self.flight_log) > 1000:
            self.flight_log.pop(0)
        return heartbeat

    def export_flight_data(self) -> pd.DataFrame:
        if not self.flight_log:
            return pd.DataFrame()
        data = [{
            'timestamp': h.timestamp,
            'flight_time': h.flight_time,
            'lat': h.lat,
            'lng': h.lng,
            'altitude': h.altitude,
            'voltage': h.voltage,
            'satellites': h.satellites,
            'speed': h.speed,
            'progress': h.progress,
            'arrived': h.arrived,
            'safety_violation': h.safety_violation,
            'remaining_distance': h.remaining_distance
        } for h in self.flight_log]
        return pd.DataFrame(data)

# ==================== 地图创建 ====================
def create_planning_map(center_gcj: List[float], points_gcj: Dict, obstacles_gcj: List[Dict],
                        flight_history: Optional[List] = None, planned_path: Optional[List] = None,
                        straight_blocked: bool = True, flight_altitude: float = 50,
                        drone_pos: Optional[List] = None, direction: str = "最佳航线",
                        safety_radius: float = 5) -> folium.Map:
    tiles = config.GAODE_SATELLITE_URL
    m = folium.Map(location=[center_gcj[1], center_gcj[0]], zoom_start=16, tiles=tiles, attr="高德卫星地图")

    draw = plugins.Draw(
        export=True, position='topleft',
        draw_options={
            'polygon': {'allowIntersection': False, 'showArea': True, 'color': '#ff0000',
                        'fillColor': '#ff0000', 'fillOpacity': 0.4},
            'polyline': False, 'rectangle': False, 'circle': False, 'marker': False, 'circlemarker': False
        },
        edit_options={'edit': True, 'remove': True}
    )
    m.add_child(draw)

    for obs in obstacles_gcj:
        coords = obs.get('polygon', [])
        height = obs.get('height', 30)
        if coords and len(coords) >= 3:
            color = "red" if height > flight_altitude else "orange"
            folium.Polygon([[c[1], c[0]] for c in coords], color=color, weight=3, fill=True,
                          fill_color=color, fill_opacity=0.4, popup=f"🚧 {obs.get('name')}\n高度: {height}m").add_to(m)

    if points_gcj.get('A'):
        folium.Marker([points_gcj['A'][1], points_gcj['A'][0]], popup="🟢 起点",
                     icon=folium.Icon(color="green", icon="play", prefix="fa")).add_to(m)
        # 起点安全半径圆
        folium.Circle(
            radius=safety_radius,
            location=[points_gcj['A'][1], points_gcj['A'][0]],
            color="green", weight=2, fill=True,
            fill_color="green", fill_opacity=0.15,
            popup=f"🛡️ 起点安全半径: {safety_radius}米"
        ).add_to(m)
    
    if points_gcj.get('B'):
        folium.Marker([points_gcj['B'][1], points_gcj['B'][0]], popup="🔴 终点",
                     icon=folium.Icon(color="red", icon="stop", prefix="fa")).add_to(m)
        # 终点安全半径圆
        folium.Circle(
            radius=safety_radius,
            location=[points_gcj['B'][1], points_gcj['B'][0]],
            color="red", weight=2, fill=True,
            fill_color="red", fill_opacity=0.15,
            popup=f"🛡️ 终点安全半径: {safety_radius}米"
        ).add_to(m)

    if planned_path and len(planned_path) > 1:
        path_locations = [[p[1], p[0]] for p in planned_path]
        line_color = "purple" if "向左" in direction else "orange" if "向右" in direction else "green"
        folium.PolyLine(path_locations, color=line_color, weight=5, opacity=0.9, popup=f"✈️ {direction}").add_to(m)
        for i, point in enumerate(planned_path[1:-1]):
            folium.CircleMarker([point[1], point[0]], radius=5, color=line_color, fill=True,
                               fill_color="white", fill_opacity=0.8, popup=f"航点 {i+1}").add_to(m)

    if points_gcj.get('A') and points_gcj.get('B'):
        line = [[points_gcj['A'][1], points_gcj['A'][0]], [points_gcj['B'][1], points_gcj['B'][0]]]
        if straight_blocked:
            folium.PolyLine(line, color="gray", weight=2, opacity=0.4, dash_array='5,5', popup="⚠️ 直线被阻挡").add_to(m)
        else:
            folium.PolyLine(line, color="blue", weight=2, opacity=0.5, dash_array='5,5', popup="直线航线").add_to(m)

    pos = drone_pos if drone_pos else points_gcj.get('A')
    if pos:
        folium.Circle(radius=safety_radius, location=[pos[1], pos[0]], color="blue", weight=2, fill=True,
                     fill_color="blue", fill_opacity=0.2, popup=f"🛡️ 安全半径: {safety_radius}米").add_to(m)

    if flight_history and len(flight_history) > 1:
        trail = [[p[1], p[0]] for p in flight_history if len(p) >= 2]
        if len(trail) > 1:
            folium.PolyLine(trail, color="orange", weight=2, opacity=0.6, popup="历史轨迹").add_to(m)
    
    return m

# ==================== 辅助UI函数 ====================
def init_session_state():
    defaults = {
        'points_gcj': {'A': config.DEFAULT_A_GCJ.copy(), 'B': config.DEFAULT_B_GCJ.copy()},
        'obstacles_gcj': load_obstacles(),
        'heartbeat_sim': HeartbeatSimulator(config.DEFAULT_A_GCJ.copy()),
        'comm_sim': CommunicationSimulator(),
        'last_hb_time': time.time(),
        'simulation_running': False,
        'flight_history': [],
        'planned_path': None,
        'last_flight_altitude': 50,
        'pending_obstacle': None,
        'current_direction': "最佳航线",
        'safety_radius': config.DEFAULT_SAFETY_RADIUS_METERS,
        'auto_backup': True,
        'show_rename_dialog': False,
        'waiting_for_start_point': False,
        'waiting_for_end_point': False,
        'temp_click_point': None,
        'conv_result': None,
        'batch_result': None,
        'offset_result': None
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value
    for obs in st.session_state.obstacles_gcj:
        if 'height' not in obs:
            obs['height'] = 30
        if 'selected' not in obs:
            obs['selected'] = False

def check_straight_blocked(points_gcj: Dict, obstacles_gcj: List[Dict], flight_altitude: float) -> Tuple[bool, int]:
    blocked = False
    high_count = 0
    for obs in obstacles_gcj:
        if obs.get('height', 30) > flight_altitude:
            high_count += 1
            coords = obs.get('polygon', [])
            if coords and line_intersects_polygon(points_gcj['A'], points_gcj['B'], coords):
                blocked = True
    return blocked, high_count

def render_sidebar() -> Tuple[str, int, float, bool]:
    st.sidebar.title("🎛️ 导航菜单")
    page = st.sidebar.radio("选择功能模块", ["🗺️ 航线规划", "📡 飞行监控", "🔗 通信拓扑", "🚧 障碍物管理"])
    st.sidebar.markdown("---")
    st.sidebar.subheader("⚡ 无人机速度设置")
    drone_speed = st.sidebar.slider("飞行速度系数", 0, 100, 50, 5)
    st.sidebar.markdown("---")
    st.sidebar.subheader("✈️ 无人机飞行高度")
    flight_alt = st.sidebar.slider("飞行高度 (m)", 0, 120, 30, 5)
    st.sidebar.markdown("---")
    st.sidebar.subheader("🛡️ 安全半径设置")
    new_safety_radius = st.sidebar.slider("安全半径 (米)", 1, 20, st.session_state.safety_radius, 1)
    if new_safety_radius != st.session_state.safety_radius:
        st.session_state.safety_radius = new_safety_radius
        st.session_state.heartbeat_sim.safety_radius = new_safety_radius
        if st.session_state.planned_path and st.session_state.points_gcj['A'] and st.session_state.points_gcj['B']:
            st.session_state.planned_path = create_avoidance_path(
                st.session_state.points_gcj['A'], st.session_state.points_gcj['B'],
                st.session_state.obstacles_gcj, st.session_state.last_flight_altitude,
                st.session_state.current_direction, new_safety_radius)
    st.sidebar.markdown("---")
    st.sidebar.subheader("💾 自动保存")
    auto_save = st.sidebar.checkbox("自动保存障碍物", st.session_state.auto_backup)
    return page, drone_speed, flight_alt, auto_save

# ==================== 通信拓扑页面 ====================
def render_communication_page():
    st.header("🔗 通信链路拓扑与数据流")
    comm = st.session_state.comm_sim

    st.markdown("### 🖥️ 系统节点状态")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        gcs_status = "🟢 在线" if comm.gcs_online else "🔴 离线"
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    border-radius: 15px; padding: 20px; text-align: center; color: white;">
            <h2>📡 GCS</h2>
            <h3>{gcs_status}</h3>
            <p style="font-size: 12px; margin: 5px 0;">地面站</p>
            <p style="font-size: 11px; opacity: 0.8;">{comm.gcs_ip}</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        obc_status = "🟢 在线" if comm.obc_online else "🔴 离线"
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                    border-radius: 15px; padding: 20px; text-align: center; color: white;">
            <h2>💻 OBC</h2>
            <h3>{obc_status}</h3>
            <p style="font-size: 12px; margin: 5px 0;">机载计算机</p>
            <p style="font-size: 11px; opacity: 0.8;">{comm.obc_ip} | Raspberry Pi 4</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        fcu_status = "🟢 在线" if comm.fcu_online else "🔴 离线"
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
                    border-radius: 15px; padding: 20px; text-align: center; color: white;">
            <h2>🎮 FCU</h2>
            <h3>{fcu_status}</h3>
            <p style="font-size: 12px; margin: 5px 0;">飞控</p>
            <p style="font-size: 11px; opacity: 0.8;">{comm.fcu_ip} | PX4 / ArduPilot</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("📡 通信链路拓扑")
    
    top_col1, top_col2, top_col3 = st.columns([1, 2, 1])
    with top_col1:
        st.markdown("""
        <div style="text-align: center;">
            <div style="background: #f0f2f6; border-radius: 10px; padding: 15px; margin: 10px;">
                <h3>🖥️ GCS</h3>
                <p><strong>地面站</strong></p>
                <p style="font-size: 12px; color: #666;">192.168.1.100</p>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with top_col2:
        gcs_obc_status = comm.check_link_status("GCS", "OBC")
        obc_fcu_status = comm.check_link_status("OBC", "FCU")
        gcs_obc_color = "#4CAF50" if gcs_obc_status else "#f44336"
        obc_fcu_color = "#4CAF50" if obc_fcu_status else "#f44336"
        
        st.markdown(f"""
        <div style="text-align: center;">
            <div style="background: #f8f9fa; border-radius: 10px; padding: 15px;">
                <p><strong>🔗 链路状态</strong></p>
                <div style="margin: 15px 0;">
                    <div style="display: inline-block; width: 40px; height: 2px; background: {gcs_obc_color};"></div>
                    <span style="margin: 0 10px;">GCS ↔ OBC</span>
                    <div style="display: inline-block; width: 40px; height: 2px; background: {gcs_obc_color};"></div>
                    <br>
                    <span style="font-size: 12px;">UDP:14550 | {"🟢 已连接" if gcs_obc_status else "🔴 断开"}</span>
                    <br>
                    <span style="font-size: 11px; color: #666;">延迟: {comm.gcs_obc_latency}ms</span>
                </div>
                <div style="margin: 15px 0;">
                    <div style="display: inline-block; width: 40px; height: 2px; background: {obc_fcu_color};"></div>
                    <span style="margin: 0 10px;">OBC ↔ FCU</span>
                    <div style="display: inline-block; width: 40px; height: 2px; background: {obc_fcu_color};"></div>
                    <br>
                    <span style="font-size: 12px;">MAVLink | {"🟢 已连接" if obc_fcu_status else "🔴 断开"}</span>
                    <br>
                    <span style="font-size: 11px; color: #666;">延迟: {comm.obc_fcu_latency}ms</span>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    with top_col3:
        st.markdown("""
        <div style="text-align: center;">
            <div style="background: #f0f2f6; border-radius: 10px; padding: 15px; margin: 10px;">
                <h3>🎮 FCU</h3>
                <p><strong>飞控</strong></p>
                <p style="font-size: 12px; color: #666;">192.168.1.102</p>
                <p style="font-size: 11px;">PX4 / ArduPilot</p>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.subheader("📊 链路统计")
    stats = comm.get_statistics()
    
    metric_cols = st.columns(4)
    with metric_cols[0]:
        st.metric("📤 发送包数", f"{stats['sent']:,}")
    with metric_cols[1]:
        st.metric("📥 接收包数", f"{stats['received']:,}")
    with metric_cols[2]:
        st.metric("❌ 丢包数", f"{stats['lost']:,}")
    with metric_cols[3]:
        success_rate = stats['success_rate']
        st.metric("✅ 成功率", f"{success_rate:.1f}%")
    
    stat_cols = st.columns(3)
    with stat_cols[0]:
        st.metric("⚡ GCS-OBC延迟", f"{stats['gcs_obc_latency']}ms")
    with stat_cols[1]:
        st.metric("⚡ OBC-FCU延迟", f"{stats['obc_fcu_latency']}ms")
    with stat_cols[2]:
        loss_rate = stats['packet_loss_rate'] * 100
        st.metric("📉 丢包率", f"{loss_rate:.1f}%")

    st.markdown("---")
    st.subheader("🎮 链路控制")
    control_cols = st.columns(4)
    with control_cols[0]:
        if st.button("🔄 重置统计", use_container_width=True, type="primary"):
            comm.reset_statistics()
            st.success("✅ 统计已重置")
            st.rerun()
    with control_cols[1]:
        new_gcs_latency = st.slider("GCS-OBC延迟(ms)", 5, 100, comm.gcs_obc_latency, 5, key="gcs_latency")
        if new_gcs_latency != comm.gcs_obc_latency:
            comm.gcs_obc_latency = new_gcs_latency
    with control_cols[2]:
        new_obc_latency = st.slider("OBC-FCU延迟(ms)", 5, 100, comm.obc_fcu_latency, 5, key="obc_latency")
        if new_obc_latency != comm.obc_fcu_latency:
            comm.obc_fcu_latency = new_obc_latency
    with control_cols[3]:
        new_loss_rate = st.slider("丢包率(%)", 0.0, 5.0, comm.packet_loss_rate * 100, 0.1, key="loss_rate") / 100
        if new_loss_rate != comm.packet_loss_rate:
            comm.packet_loss_rate = new_loss_rate

    st.markdown("---")
    st.subheader("📋 通信日志")
    
    col_flow1, col_flow2 = st.columns(2)
    with col_flow1:
        st.info("📤 **GCS → OBC → FCU**\n\n航线规划指令下发流程")
    with col_flow2:
        st.info("📥 **FCU → OBC → GCS**\n\n飞行状态上报流程")
    st.markdown("---")
    
    log_mode = st.radio("显示模式", ["📋 表格视图", "📝 详细视图"], horizontal=True)
    
    if log_mode == "📋 表格视图":
        if comm.logs:
            log_data = []
            for i, log in enumerate(comm.logs[:50]):
                log_data.append({
                    "序号": i + 1,
                    "时间": log.timestamp,
                    "方向": log.direction,
                    "消息": log.message,
                    "详情": log.details if log.details else "-"
                })
            df = pd.DataFrame(log_data)
            st.dataframe(df, use_container_width=True, height=400)
        else:
            st.info("📭 暂无通信日志")
        
        col_clear1, col_clear2, col_clear3 = st.columns([1, 1, 1])
        with col_clear2:
            if st.button("🗑️ 清空所有日志", use_container_width=True, type="secondary"):
                comm.logs.clear()
                comm.planning_records.clear()
                st.success("✅ 日志已清空")
                st.rerun()
    else:
        tab1, tab2, tab3 = st.tabs(["📤 下行指令 (GCS→OBC→FCU)", "📥 上行状态 (FCU→OBC→GCS)", "📋 规划记录"])
        
        with tab1:
            st.caption("航线规划指令下发流程")
            gcs_obc_logs = [log for log in comm.logs if log.direction == "GCS→OBC"]
            if gcs_obc_logs:
                st.markdown("#### 📡 GCS → OBC")
                for log in gcs_obc_logs[:15]:
                    st.markdown(f"""
                    <div style="background: #e3f2fd; border-left: 4px solid #2196f3; padding: 8px; margin: 5px 0; border-radius: 5px;">
                        <code>[{log.timestamp}]</code> <strong>{log.message}</strong>
                        {f'<br><span style="color: #666; font-size: 12px;">📝 {log.details}</span>' if log.details else ''}
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("暂无 GCS → OBC 日志")
            
            obc_fcu_logs = [log for log in comm.logs if log.direction == "OBC→FCU"]
            if obc_fcu_logs:
                st.markdown("#### 🖥️ OBC → FCU")
                for log in obc_fcu_logs[:15]:
                    st.markdown(f"""
                    <div style="background: #e8f5e9; border-left: 4px solid #4caf50; padding: 8px; margin: 5px 0; border-radius: 5px;">
                        <code>[{log.timestamp}]</code> <strong>{log.message}</strong>
                        {f'<br><span style="color: #666; font-size: 12px;">📝 {log.details}</span>' if log.details else ''}
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("暂无 OBC → FCU 日志")
        
        with tab2:
            st.caption("飞行状态上报流程")
            fcu_obc_logs = [log for log in comm.logs if log.direction == "FCU→OBC"]
            if fcu_obc_logs:
                st.markdown("#### 🎮 FCU → OBC")
                for log in fcu_obc_logs[:20]:
                    st.markdown(f"""
                    <div style="background: #fff3e0; border-left: 4px solid #ff9800; padding: 8px; margin: 5px 0; border-radius: 5px;">
                        <code>[{log.timestamp}]</code> <strong>{log.message}</strong>
                        {f'<br><span style="color: #666; font-size: 12px;">📝 {log.details}</span>' if log.details else ''}
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("暂无 FCU → OBC 日志")
            
            obc_gcs_logs = [log for log in comm.logs if log.direction == "OBC→GCS"]
            if obc_gcs_logs:
                st.markdown("#### 💻 OBC → GCS")
                for log in obc_gcs_logs[:20]:
                    st.markdown(f"""
                    <div style="background: #f3e5f5; border-left: 4px solid #9c27b0; padding: 8px; margin: 5px 0; border-radius: 5px;">
                        <code>[{log.timestamp}]</code> <strong>{log.message}</strong>
                        {f'<br><span style="color: #666; font-size: 12px;">📝 {log.details}</span>' if log.details else ''}
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("暂无 OBC → GCS 日志")
        
        with tab3:
            st.caption("航线规划记录")
            if comm.planning_records:
                for record in comm.planning_records[:15]:
                    st.markdown(f"""
                    <div style="background: #e0f7fa; border-left: 4px solid #00bcd4; padding: 10px; margin: 8px 0; border-radius: 5px;">
                        <code>[{record.get('timestamp', '')}]</code>
                        <strong>✈️ {record.get('message', '')}</strong>
                        {f'<br><span style="color: #006064; font-size: 12px;">📊 {record.get("details", "")}</span>' if record.get('details') else ''}
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.info("暂无航线规划记录")
        
        col_clear1, col_clear2, col_clear3 = st.columns([1, 1, 1])
        with col_clear2:
            if st.button("🗑️ 清空所有日志", use_container_width=True, type="secondary"):
                comm.logs.clear()
                comm.planning_records.clear()
                st.success("✅ 日志已清空")
                st.rerun()

# ==================== 航线规划页面 ====================
def render_planning_page(drone_speed: int, flight_alt: float, auto_save: bool):
    """航线规划主页面 - 整合坐标转换功能"""
    st.header("🗺️ 航线规划")
    
    tab1, tab2 = st.tabs(["✈️ 航线规划", "🔄 坐标转换工具"])
    
    with tab1:
        render_planning_tab(drone_speed, flight_alt, auto_save)
    with tab2:
        render_coordinate_conversion_tab()

def render_planning_tab(drone_speed: int, flight_alt: float, auto_save: bool):
    """航线规划标签页"""
    blocked, high = check_straight_blocked(st.session_state.points_gcj, st.session_state.obstacles_gcj, flight_alt)
    
    status_cols = st.columns([2, 1])
    with status_cols[0]:
        if blocked:
            st.warning(f"⚠️ 有 {high} 个障碍物高于飞行高度({flight_alt}m)，需要绕行")
        else:
            st.success("✅ 直线航线畅通无阻")
    with status_cols[1]:
        st.info(f"🛡️ 安全半径: {st.session_state.safety_radius}m")
    
    st.info("📝 点击地图左上角📐图标 → 选择多边形 → 围绕建筑物绘制 → 双击完成 → 输入高度并保存")
    
    col1, col2 = st.columns([1, 1.5])
    with col1:
        render_planning_controls(flight_alt, drone_speed, auto_save)
    with col2:
        render_planning_map_view(flight_alt, blocked)

def render_planning_controls(flight_alt: float, drone_speed: int, auto_save: bool):
    """规划控制面板"""
    with st.expander("📍 起点/终点设置", expanded=True):
        render_point_settings()
    
    with st.expander("🤖 路径规划策略", expanded=True):
        render_path_strategy(flight_alt)
    
    st.subheader("✈️ 飞行控制")
    param_cols = st.columns(3)
    with param_cols[0]:
        st.metric("飞行高度", f"{flight_alt} m")
    with param_cols[1]:
        st.metric("速度系数", f"{drone_speed}%")
    with param_cols[2]:
        st.metric("安全半径", f"{st.session_state.safety_radius} m")
    
    btn_col1, btn_col2 = st.columns(2)
    with btn_col1:
        if st.button("▶️ 开始飞行", use_container_width=True, type="primary"):
            start_flight(flight_alt, drone_speed)
    with btn_col2:
        if st.button("⏹️ 停止飞行", use_container_width=True):
            stop_flight()
    
    st.markdown("---")
    render_current_coords_info()

def render_current_coords_info():
    """显示当前坐标信息"""
    st.subheader("📍 当前坐标信息")
    a, b = st.session_state.points_gcj['A'], st.session_state.points_gcj['B']
    
    coord_cols = st.columns(2)
    with coord_cols[0]:
        st.markdown(f"""
        <div style="background: #f0f9f0; border-radius: 10px; padding: 10px; border-left: 4px solid #4CAF50;">
            <span style="font-size: 12px; color: #666;">🟢 起点 A</span><br>
            <code style="font-size: 12px;">经度: {a[0]:.8f}</code><br>
            <code style="font-size: 12px;">纬度: {a[1]:.8f}</code>
        </div>
        """, unsafe_allow_html=True)
    
    with coord_cols[1]:
        st.markdown(f"""
        <div style="background: #fff0f0; border-radius: 10px; padding: 10px; border-left: 4px solid #f44336;">
            <span style="font-size: 12px; color: #666;">🔴 终点 B</span><br>
            <code style="font-size: 12px;">经度: {b[0]:.8f}</code><br>
            <code style="font-size: 12px;">纬度: {b[1]:.8f}</code>
        </div>
        """, unsafe_allow_html=True)
    
    dist = math.hypot(b[0] - a[0], b[1] - a[1]) * 111000
    info_cols = st.columns(2)
    with info_cols[0]:
        st.metric("📏 直线距离", f"{dist:.0f} 米")
    with info_cols[1]:
        if st.session_state.planned_path:
            total_dist = calculate_path_length(st.session_state.planned_path) * 111000
            delta_dist = total_dist - dist
            st.metric("🛣️ 规划路径总长", f"{total_dist:.0f} 米", delta=f"+{delta_dist:.0f}m" if delta_dist > 0 else None)
        else:
            st.metric("🛣️ 规划路径总长", "未规划")
    
    if st.session_state.planned_path and len(st.session_state.planned_path) > 2:
        waypoint_count = len(st.session_state.planned_path) - 2
        st.caption(f"🎯 包含 {waypoint_count} 个绕行航点")

def render_point_settings():
    """起点/终点设置"""
    st.markdown("#### 🎯 设置方式")
    mode = st.radio("选择方式", ["✏️ 经纬度输入", "🖱️ 鼠标点击"], horizontal=True, key="point_setting_mode", label_visibility="collapsed")
    
    if mode == "✏️ 经纬度输入":
        render_coordinate_input()
    else:
        render_mouse_click_setting()

def render_coordinate_input():
    """经纬度输入"""
    st.markdown("**🟢 起点 A**")
    col_a1, col_a2, col_a3 = st.columns([1, 1, 1])
    with col_a1:
        a_lat = st.number_input("纬度", value=st.session_state.points_gcj['A'][1], format="%.6f", key="a_lat", step=0.000001, label_visibility="collapsed", placeholder="纬度")
    with col_a2:
        a_lng = st.number_input("经度", value=st.session_state.points_gcj['A'][0], format="%.6f", key="a_lng", step=0.000001, label_visibility="collapsed", placeholder="经度")
    with col_a3:
        if st.button("📍 设置A点", use_container_width=True, key="set_a"):
            st.session_state.points_gcj['A'] = [a_lng, a_lat]
            update_path_after_point_change()
            st.success("✅ 起点已更新")
            st.rerun()
    
    st.markdown("**🔴 终点 B**")
    col_b1, col_b2, col_b3 = st.columns([1, 1, 1])
    with col_b1:
        b_lat = st.number_input("纬度", value=st.session_state.points_gcj['B'][1], format="%.6f", key="b_lat", step=0.000001, label_visibility="collapsed", placeholder="纬度")
    with col_b2:
        b_lng = st.number_input("经度", value=st.session_state.points_gcj['B'][0], format="%.6f", key="b_lng", step=0.000001, label_visibility="collapsed", placeholder="经度")
    with col_b3:
        if st.button("📍 设置B点", use_container_width=True, key="set_b"):
            st.session_state.points_gcj['B'] = [b_lng, b_lat]
            update_path_after_point_change()
            st.success("✅ 终点已更新")
            st.rerun()
    
    col_r1, col_r2 = st.columns(2)
    with col_r1:
        if st.button("🔄 重置默认起点", use_container_width=True):
            st.session_state.points_gcj['A'] = config.DEFAULT_A_GCJ.copy()
            update_path_after_point_change()
            st.rerun()
    with col_r2:
        if st.button("🔄 重置默认终点", use_container_width=True):
            st.session_state.points_gcj['B'] = config.DEFAULT_B_GCJ.copy()
            update_path_after_point_change()
            st.rerun()

def render_mouse_click_setting():
    """鼠标点击设置"""
    st.info("💡 点击地图上的任意位置设置起点或终点")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🎯 设置起点", use_container_width=True, type="primary"):
            st.session_state.waiting_for_start_point = True
            st.session_state.waiting_for_end_point = False
            st.rerun()
    with col2:
        if st.button("📍 设置终点", use_container_width=True, type="primary"):
            st.session_state.waiting_for_end_point = True
            st.session_state.waiting_for_start_point = False
            st.rerun()
    
    if st.session_state.waiting_for_start_point:
        st.warning("⏳ 等待设置起点... 请点击地图")
    elif st.session_state.waiting_for_end_point:
        st.warning("⏳ 等待设置终点... 请点击地图")
    
    if st.session_state.waiting_for_start_point or st.session_state.waiting_for_end_point:
        if st.button("❌ 取消", use_container_width=True):
            st.session_state.waiting_for_start_point = False
            st.session_state.waiting_for_end_point = False
            st.rerun()

def update_path_after_point_change():
    """更新路径"""
    st.session_state.planned_path = create_avoidance_path(
        st.session_state.points_gcj['A'], st.session_state.points_gcj['B'],
        st.session_state.obstacles_gcj, st.session_state.last_flight_altitude,
        st.session_state.current_direction, st.session_state.safety_radius)

def render_path_strategy(flight_alt: float):
    """路径规划策略"""
    st.markdown("**绕行方向**")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        is_best = st.session_state.current_direction == "最佳航线"
        if st.button("🔄 最佳航线", use_container_width=True, type="primary" if is_best else "secondary"):
            st.session_state.current_direction = "最佳航线"
            st.session_state.planned_path = create_avoidance_path(
                st.session_state.points_gcj['A'], st.session_state.points_gcj['B'],
                st.session_state.obstacles_gcj, flight_alt, "最佳航线", st.session_state.safety_radius)
            st.success("✅ 已切换到最佳航线")
            st.rerun()
    
    with col2:
        is_left = st.session_state.current_direction == "向左绕行"
        if st.button("⬅️ 向左绕行", use_container_width=True, type="primary" if is_left else "secondary"):
            st.session_state.current_direction = "向左绕行"
            st.session_state.planned_path = create_avoidance_path(
                st.session_state.points_gcj['A'], st.session_state.points_gcj['B'],
                st.session_state.obstacles_gcj, flight_alt, "向左绕行", st.session_state.safety_radius)
            st.success("✅ 已切换到向左绕行")
            st.rerun()
    
    with col3:
        is_right = st.session_state.current_direction == "向右绕行"
        if st.button("➡️ 向右绕行", use_container_width=True, type="primary" if is_right else "secondary"):
            st.session_state.current_direction = "向右绕行"
            st.session_state.planned_path = create_avoidance_path(
                st.session_state.points_gcj['A'], st.session_state.points_gcj['B'],
                st.session_state.obstacles_gcj, flight_alt, "向右绕行", st.session_state.safety_radius)
            st.success("✅ 已切换到向右绕行")
            st.rerun()
    
    st.info(f"📌 当前策略: **{st.session_state.current_direction}**")
    
    if st.button("🔄 重新规划路径", use_container_width=True):
        with st.spinner("规划中..."):
            st.session_state.planned_path = create_avoidance_path(
                st.session_state.points_gcj['A'], st.session_state.points_gcj['B'],
                st.session_state.obstacles_gcj, flight_alt,
                st.session_state.current_direction, st.session_state.safety_radius)
        if st.session_state.planned_path:
            st.success("✅ 路径已重新规划")
            st.rerun()

def start_flight(flight_alt: float, drone_speed: int):
    """开始飞行"""
    if not st.session_state.points_gcj['A'] or not st.session_state.points_gcj['B']:
        st.error("请先设置起点和终点")
        return
    
    path = st.session_state.planned_path or [st.session_state.points_gcj['A'], st.session_state.points_gcj['B']]
    comm = st.session_state.comm_sim
    total = calculate_path_length(path) * 111000
    
    comm.add_planning_record({"message": "开始航线规划", "details": f"障碍物数量: {len(st.session_state.obstacles_gcj)}"})
    comm.add_planning_record({"message": "航线规划完成", "details": f"航点数: {len(path)} | 路径长度: {total:.1f}m"})
    comm.add_planning_record({"message": "导航目标", "details": f"起点→终点 | 目标高度: {flight_alt}m"})
    comm.send_message("GCS", "OBC", "START_MISSION")
    comm.send_message("OBC", "FCU", "UPLOAD_MISSION", f"航点数量: {len(path)}")
    
    st.session_state.heartbeat_sim.set_path(path, flight_alt, drone_speed, st.session_state.safety_radius)
    st.session_state.simulation_running = True
    st.session_state.flight_history = []
    
    comm.send_message("FCU", "OBC", "ACK", "Mode: AUTO")
    comm.send_message("OBC", "GCS", "ACK", "任务已开始")
    
    waypoint_msg = f'路径中有 {len(path)-2} 个绕行点' if len(path) > 2 else '直线飞行'
    st.success(f"🚁 飞行已开始！{waypoint_msg}")
    st.rerun()

def stop_flight():
    """停止飞行"""
    st.session_state.simulation_running = False
    st.session_state.heartbeat_sim.simulating = False
    st.session_state.comm_sim.send_message("GCS", "OBC", "STOP_MISSION", "用户停止飞行")
    st.info("✈️ 飞行已停止")
    st.rerun()

def render_planning_map_view(flight_alt: float, straight_blocked: bool):
    """规划地图视图"""
    st.subheader("🗺️ 规划地图")
    
    if straight_blocked:
        st.caption(f"🎯 当前避障策略: {st.session_state.current_direction}")
        st.caption("🟢 绿色=最佳航线 | 🟣 紫色=向左绕行 | 🟠 橙色=向右绕行 | 🔵 蓝色=安全半径")
    
    flight_trail = [[hb.lng, hb.lat] for hb in st.session_state.heartbeat_sim.history[:20]]
    center = st.session_state.points_gcj['A'] or config.SCHOOL_CENTER_GCJ
    
    if st.session_state.planned_path is None:
        st.session_state.planned_path = create_avoidance_path(
            st.session_state.points_gcj['A'], st.session_state.points_gcj['B'],
            st.session_state.obstacles_gcj, flight_alt,
            st.session_state.current_direction, st.session_state.safety_radius)
    
    drone_pos = st.session_state.heartbeat_sim.current_pos if st.session_state.heartbeat_sim.simulating else None
    
    m = create_planning_map(center, st.session_state.points_gcj, st.session_state.obstacles_gcj,
                            flight_trail, st.session_state.planned_path, straight_blocked,
                            flight_alt, drone_pos, st.session_state.current_direction, 
                            st.session_state.safety_radius)
    
    output = st_folium(m, width=700, height=550, returned_objects=["last_active_drawing", "last_clicked"])
    handle_map_click(output)
    handle_drawing_output(output)

def handle_map_click(output):
    """处理地图点击"""
    if output and output.get("last_clicked"):
        clicked = output["last_clicked"]
        if clicked and isinstance(clicked, dict):
            lng = clicked.get("lng")
            lat = clicked.get("lat")
            if lng is not None and lat is not None:
                if st.session_state.waiting_for_start_point:
                    st.session_state.points_gcj['A'] = [lng, lat]
                    update_path_after_point_change()
                    st.session_state.waiting_for_start_point = False
                    st.success(f"✅ 起点已设置: ({lng:.6f}, {lat:.6f})")
                    st.rerun()
                elif st.session_state.waiting_for_end_point:
                    st.session_state.points_gcj['B'] = [lng, lat]
                    update_path_after_point_change()
                    st.session_state.waiting_for_end_point = False
                    st.success(f"✅ 终点已设置: ({lng:.6f}, {lat:.6f})")
                    st.rerun()

def handle_drawing_output(output):
    """处理地图绘制"""
    if output and output.get("last_active_drawing"):
        last = output["last_active_drawing"]
        if last and last.get("geometry") and last["geometry"].get("type") == "Polygon":
            coords = last["geometry"].get("coordinates", [])
            if coords and len(coords) > 0:
                poly = [[p[0], p[1]] for p in coords[0]]
                if len(poly) >= 3 and st.session_state.pending_obstacle is None:
                    if validate_polygon(poly):
                        st.session_state.pending_obstacle = poly
                        st.rerun()
    
    if st.session_state.pending_obstacle is not None:
        render_obstacle_dialog()

def render_obstacle_dialog():
    """障碍物添加对话框"""
    st.markdown("---")
    st.subheader("📝 添加新障碍物")
    st.info(f"已检测到新绘制的多边形，共 {len(st.session_state.pending_obstacle)} 个顶点")
    
    col1, col2 = st.columns(2)
    with col1:
        new_name = st.text_input("障碍物名称", f"建筑物{len(st.session_state.obstacles_gcj) + 1}")
    with col2:
        new_height = st.number_input("障碍物高度 (米)", 1, 200, 30, 5, key="height_input")
    
    col_ok, col_cancel = st.columns(2)
    with col_ok:
        if st.button("✅ 确认添加", use_container_width=True, type="primary"):
            new_obstacle = {
                "name": new_name,
                "polygon": st.session_state.pending_obstacle,
                "height": new_height,
                "selected": False,
                "id": f"obs_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(st.session_state.obstacles_gcj)}",
                "created_time": datetime.now().isoformat()
            }
            st.session_state.obstacles_gcj.append(new_obstacle)
            if st.session_state.auto_backup:
                save_obstacles(st.session_state.obstacles_gcj)
            st.session_state.planned_path = create_avoidance_path(
                st.session_state.points_gcj['A'], st.session_state.points_gcj['B'],
                st.session_state.obstacles_gcj, st.session_state.last_flight_altitude,
                st.session_state.current_direction, st.session_state.safety_radius)
            st.session_state.pending_obstacle = None
            st.success(f"✅ 已添加 {new_name}，高度 {new_height} 米")
            st.rerun()
    with col_cancel:
        if st.button("❌ 取消", use_container_width=True):
            st.session_state.pending_obstacle = None
            st.rerun()

# ==================== 坐标转换工具标签页 ====================
def render_coordinate_conversion_tab():
    """坐标转换工具标签页"""
    st.subheader("🔄 WGS-84 ↔ GCJ-02 坐标转换")
    st.caption("WGS-84 (GPS) ↔ GCJ-02 (高德/腾讯/谷歌中国)")
    
    convert_type = st.radio("转换模式", ["📍 单点转换", "📊 批量转换"], horizontal=True, key="conv_type")
    st.markdown("---")
    
    if convert_type == "📍 单点转换":
        render_single_point_conversion()
    else:
        render_batch_conversion()

def render_single_point_conversion():
    """单点转换"""
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("#### 📥 输入坐标")
        direction = st.radio("方向", ["WGS-84 → GCJ-02", "GCJ-02 → WGS-84"], horizontal=True, key="single_direction")
        lng = st.number_input("经度", value=118.748726, format="%.6f", key="single_lng")
        lat = st.number_input("纬度", value=32.233881, format="%.6f", key="single_lat")
        
        if st.button("🔄 执行转换", type="primary", use_container_width=True):
            try:
                if direction == "WGS-84 → GCJ-02":
                    out_lng, out_lat = CoordinateConverter.wgs84_to_gcj02(lng, lat)
                    st.session_state.conv_result = {
                        "input": (lng, lat), "output": (out_lng, out_lat),
                        "direction": "WGS-84 → GCJ-02"
                    }
                else:
                    out_lng, out_lat = CoordinateConverter.gcj02_to_wgs84(lng, lat)
                    st.session_state.conv_result = {
                        "input": (lng, lat), "output": (out_lng, out_lat),
                        "direction": "GCJ-02 → WGS-84"
                    }
            except Exception as e:
                st.error(f"转换失败: {e}")
    
    with col2:
        st.markdown("#### 📤 转换结果")
        if st.session_state.get("conv_result"):
            res = st.session_state.conv_result
            st.success(f"**{res['direction']}**")
            
            delta_lng = res['output'][0] - res['input'][0]
            delta_lat = res['output'][1] - res['input'][1]
            delta_lng_m = delta_lng * 111000 * math.cos(math.radians(res['input'][1]))
            delta_lat_m = delta_lat * 111000
            
            st.markdown(f"""
            | 项目 | 经度 | 纬度 |
            |------|------|------|
            | 输入 | `{res['input'][0]:.8f}` | `{res['input'][1]:.8f}` |
            | 输出 | `{res['output'][0]:.8f}` | `{res['output'][1]:.8f}` |
            | 偏移 | {delta_lng_m:.2f}米 | {delta_lat_m:.2f}米 |
            """)
            
            st.markdown("---")
            st.markdown("#### 🎯 应用到航线")
            col_apply1, col_apply2 = st.columns(2)
            with col_apply1:
                if st.button("📌 设为起点", use_container_width=True):
                    st.session_state.points_gcj['A'] = [res['output'][0], res['output'][1]]
                    update_path_after_point_change()
                    st.success("✅ 已设为起点")
                    st.rerun()
            with col_apply2:
                if st.button("📍 设为终点", use_container_width=True):
                    st.session_state.points_gcj['B'] = [res['output'][0], res['output'][1]]
                    update_path_after_point_change()
                    st.success("✅ 已设为终点")
                    st.rerun()
        else:
            st.info("点击「执行转换」查看结果")

def render_batch_conversion():
    """批量转换"""
    st.markdown("#### 📥 输入坐标")
    st.caption("每行格式：经度,纬度")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        direction = st.radio("方向", ["WGS-84 → GCJ-02", "GCJ-02 → WGS-84"], horizontal=True, key="batch_direction")
        batch_input = st.text_area("坐标列表", height=250,
                                   placeholder="118.748726,32.233881\n118.750110,32.235460\n118.749000,32.234000",
                                   key="batch_input")
        
        if st.button("📊 执行批量转换", type="primary", use_container_width=True):
            if batch_input.strip():
                lines = batch_input.strip().split('\n')
                coords = []
                invalid_lines = []
                
                for i, line in enumerate(lines, 1):
                    line = line.strip()
                    if not line:
                        continue
                    parts = line.split(',')
                    if len(parts) >= 2:
                        try:
                            lng = float(parts[0].strip())
                            lat = float(parts[1].strip())
                            coords.append((lng, lat))
                        except ValueError:
                            invalid_lines.append(i)
                    else:
                        invalid_lines.append(i)
                
                if invalid_lines:
                    st.warning(f"跳过无效行: 第{', '.join(map(str, invalid_lines))}行")
                
                if coords:
                    try:
                        if direction == "WGS-84 → GCJ-02":
                            results = CoordinateConverter.convert_batch(coords, "wgs84_to_gcj02")
                        else:
                            results = CoordinateConverter.convert_batch(coords, "gcj02_to_wgs84")
                        
                        st.session_state.batch_result = {
                            "input": coords, "output": results, "direction": direction
                        }
                    except Exception as e:
                        st.error(f"批量转换失败: {e}")
    
    with col2:
        st.markdown("#### 📤 转换结果")
        if st.session_state.get("batch_result"):
            res = st.session_state.batch_result
            st.success(f"**{res['direction']}** - 共 {len(res['input'])} 个点")
            
            result_data = []
            for i, (in_coord, out_coord) in enumerate(zip(res['input'], res['output'])):
                delta_lng = out_coord[0] - in_coord[0]
                delta_lat = out_coord[1] - in_coord[1]
                delta_lng_m = delta_lng * 111000 * math.cos(math.radians(in_coord[1]))
                delta_lat_m = delta_lat * 111000
                
                result_data.append({
                    "序号": i + 1,
                    "输入经度": f"{in_coord[0]:.8f}",
                    "输入纬度": f"{in_coord[1]:.8f}",
                    "输出经度": f"{out_coord[0]:.8f}",
                    "输出纬度": f"{out_coord[1]:.8f}",
                    "Δ经度(米)": f"{delta_lng_m:.2f}",
                    "Δ纬度(米)": f"{delta_lat_m:.2f}"
                })
            
            df = pd.DataFrame(result_data)
            st.dataframe(df, use_container_width=True, height=250)
            
            csv = df.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(label="📥 导出CSV", data=csv,
                               file_name=f"coordinate_conversion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                               mime="text/csv", use_container_width=True)
        else:
            st.info("点击「执行批量转换」查看结果")

# ==================== 飞行监控页面 ====================
def render_flight_monitoring_page(flight_alt: float, drone_speed: int):
    st.header("📡 飞行监控 - 实时心跳包")
    
    # 自动刷新控制
    auto_refresh = st.checkbox("🔄 自动刷新 (2秒)", value=True, key="auto_refresh_monitor")
    
    # 手动刷新按钮
    col_refresh1, col_refresh2 = st.columns([1, 3])
    with col_refresh1:
        if st.button("🔄 手动刷新", use_container_width=True):
            st.rerun()
    
    # 更新飞行模拟数据
    update_flight_simulation()

    if st.session_state.heartbeat_sim.history:
        latest = st.session_state.heartbeat_sim.history[0]
        
        # ========== 修复航点计算逻辑 ==========
        current_waypoint = 0
        total_waypoints = 0

        if st.session_state.planned_path and len(st.session_state.planned_path) > 1:
            total_waypoints = len(st.session_state.planned_path)
            
            if latest.arrived:
                # 已到达终点
                current_waypoint = total_waypoints
            else:
                # 根据当前位置计算当前航点
                # 方法1：基于进度计算（原有方法但修正）
                if latest.progress >= 0:
                    # 计算当前所在的航段索引
                    segment_count = len(st.session_state.planned_path) - 1
                    segment_index = int(latest.progress * segment_count)
                    
                    # 确保索引在有效范围内
                    segment_index = min(segment_index, segment_count - 1) if segment_count > 0 else 0
                    
                    # 当前航点是下一个要到达的航点
                    if latest.progress >= 0.999:  # 接近终点
                        current_waypoint = total_waypoints
                    else:
                        current_waypoint = segment_index + 1
                    
                    # 边界检查
                    current_waypoint = min(current_waypoint, total_waypoints)
                    current_waypoint = max(current_waypoint, 1)
                else:
                    current_waypoint = 1  # 起点
        
        # 调试信息（可选，用于验证）
        # st.caption(f"调试: progress={latest.progress:.3f}, total={total_waypoints}, current={current_waypoint}")
        
        # 计算航点进度百分比
        waypoint_progress_value = current_waypoint / total_waypoints if total_waypoints > 0 else 0
        
        remaining_distance = max(0, latest.remaining_distance if not latest.arrived else 0)

        estimated_arrival = "00:00" if latest.arrived else "计算中..."
        if not latest.arrived and latest.speed > 0 and remaining_distance > 0:
            eta_seconds = remaining_distance / latest.speed
            if eta_seconds < 60:
                estimated_arrival = f"{eta_seconds:.0f}秒"
            elif eta_seconds < 3600:
                estimated_arrival = f"{int(eta_seconds // 60):02d}:{int(eta_seconds % 60):02d}"
            else:
                estimated_arrival = f"{int(eta_seconds // 3600):02d}:{int((eta_seconds % 3600) // 60):02d}"

        max_flight_time = 1800
        battery_percentage = max(0, min(100, (1 - latest.flight_time / max_flight_time) * 100))
        if latest.voltage:
            voltage_percentage = ((latest.voltage - 21.0) / (22.2 - 21.0)) * 100
            battery_percentage = max(0, min(100, (battery_percentage + voltage_percentage) / 2))

        st.markdown("### ✈️ 飞行进度")
        st.progress(latest.progress if not latest.arrived else 1.0,
                    text=f"飞行进度：{int(latest.progress*100) if not latest.arrived else 100}%")

        st.markdown("### 📊 实时飞行数据")
        c1, c2, c3 = st.columns(3)
        with c1:
            waypoint_display = f"{current_waypoint} / {total_waypoints}"
            if total_waypoints > 0:
                st.metric("🎯 当前航点", waypoint_display,
                          delta=f"进度 {int(waypoint_progress_value*100)}%" if not latest.arrived else "已完成")
                # 添加航点进度条
                st.progress(waypoint_progress_value, text=f"航点进度: {int(waypoint_progress_value*100)}%")
                
                # 显示下一个航点信息
                if not latest.arrived and current_waypoint < total_waypoints:
                    next_wp = st.session_state.planned_path[current_waypoint]
                    st.caption(f"📍 下一航点: ({next_wp[0]:.6f}, {next_wp[1]:.6f})")
                elif not latest.arrived and current_waypoint == total_waypoints:
                    st.caption("🎯 即将到达终点")
            else:
                st.metric("🎯 当前航点", "0 / 0")
        with c2:
            st.metric("💨 飞行速度", f"{latest.speed:.1f} m/s", delta=f"{drone_speed}% 系数" if not latest.arrived else "已到达")
        with c3:
            st.metric("⏰ 已用时间", f"{int(latest.flight_time//60):02d}:{int(latest.flight_time%60):02d}")

        c4, c5, c6 = st.columns(3)
        with c4:
            distance_text = f"{remaining_distance/1000:.2f} km" if remaining_distance >= 1000 else f"{remaining_distance:.0f} m"
            st.metric("📏 剩余距离", distance_text if not latest.arrived else "0 m", delta="已到达!" if latest.arrived else None)
        with c5:
            st.metric("🕐 预计到达", estimated_arrival)
            if remaining_distance < 100 and remaining_distance > 0 and not latest.arrived:
                st.info("🏁 即将到达目的地！")
            elif latest.arrived:
                st.success("✅ 已到达目的地！")
        with c6:
            battery_color = "🟢" if battery_percentage > 50 else "🟡" if battery_percentage > 20 else "🔴"
            st.metric("🔋 电量模拟", f"{battery_color} {battery_percentage:.0f}%", delta=f"{latest.voltage:.1f}V")

        st.markdown("### 📍 位置与状态")
        c7, c8, c9, c10 = st.columns(4)
        with c7:
            st.metric("📍 当前位置", f"{latest.lat:.6f}, {latest.lng:.6f}")
        with c8:
            st.metric("📏 飞行高度", f"{latest.altitude} m")
        with c9:
            st.metric("🛰️ 卫星数量", f"{latest.satellites} 颗")
        with c10:
            status = "✅ 已完成" if latest.arrived else "✈️ 飞行中" if st.session_state.simulation_running else "⏸️ 已停止"
            st.metric("📌 飞行状态", status)

        if latest.safety_violation and not latest.arrived:
            st.error("⚠️ 警告：无人机进入安全半径危险区域！请立即检查！")
        if latest.arrived:
            st.success("🎉 无人机已到达目的地！飞行任务完成！")

        with st.expander("📊 飞行任务总结", expanded=True):
            c_sum1, c_sum2, c_sum3 = st.columns(3)
            with c_sum1:
                st.metric("总飞行时间", f"{int(latest.flight_time//60):02d}:{int(latest.flight_time%60):02d}")
            with c_sum2:
                total_distance = st.session_state.heartbeat_sim.total_distance * 111000
                st.metric("总飞行距离", f"{total_distance:.0f} m")
            with c_sum3:
                avg_speed = latest.speed if latest.speed > 0 else drone_speed * config.BASE_SPEED_MPS / 100
                st.metric("平均速度", f"{avg_speed:.1f} m/s")

        st.markdown("---")
        st.markdown("### 🗺️ 实时位置追踪 & 🎮 飞行控制")
        col_left, col_right = st.columns([2, 1])
        with col_left:
            display_monitor_map(flight_alt, latest)
        with col_right:
            st.markdown("#### 🎮 飞行控制")
            p1, p2 = st.columns(2)
            with p1:
                st.metric("当前飞行高度", f"{latest.altitude} m")
                st.metric("速度系数", f"{drone_speed}%")
            with p2:
                st.metric("安全半径", f"{st.session_state.safety_radius} 米")
            if st.session_state.planned_path:
                st.metric("🎯 绕行点数量", len(st.session_state.planned_path) - 2)
                total_dist = calculate_path_length(st.session_state.planned_path) * 111000
                st.caption(f"📏 规划路径总长: {total_dist:.0f} 米")

            st.markdown("**📍 当前坐标**")
            a, b = st.session_state.points_gcj['A'], st.session_state.points_gcj['B']
            st.write(f"🟢 A点: ({a[0]:.6f}, {a[1]:.6f})")
            st.write(f"🔴 B点: ({b[0]:.6f}, {b[1]:.6f})")
            dist = math.hypot(b[0] - a[0], b[1] - a[1]) * 111000
            st.caption(f"📏 直线距离: {dist:.0f} 米")
            st.caption(f"🛡️ 当前安全半径: {st.session_state.safety_radius} 米")

            col_btn1, col_btn2 = st.columns(2)
            with col_btn1:
                if st.button("▶️ 开始飞行", use_container_width=True, type="primary"):
                    if a and b:
                        path = st.session_state.planned_path or [a, b]
                        comm = st.session_state.comm_sim
                        total = calculate_path_length(path) * 111000
                        comm.add_planning_record({"message": "开始航线规划", "details": f"算法: A* | 障碍物数量: {len(st.session_state.obstacles_gcj)}"})
                        comm.add_planning_record({"message": "航线规划完成", "details": f"类型: horizontal | 航点数: {len(path)} | 路径长度: {total:.1f}m"})
                        comm.add_planning_record({"message": "导航目标", "details": f"起点: {a} | 终点: {b} | 目标高度: {flight_alt}m"})
                        comm.send_message("GCS", "OBC", "START_MISSION", f"起点: {a}, 终点: {b}")
                        comm.send_message("OBC", "FCU", "UPLOAD_MISSION", f"航点数量: {len(path)}")
                        st.session_state.heartbeat_sim.set_path(path, flight_alt, drone_speed, st.session_state.safety_radius)
                        st.session_state.simulation_running = True
                        st.session_state.flight_history = []
                        comm.send_message("FCU", "OBC", "ACK", "Mode: AUTO")
                        comm.send_message("OBC", "GCS", "ACK", "任务已开始")
                        st.success(f"🚁 飞行已开始！{'路径中有' + str(len(path)-2) + '个绕行点' if len(path)>2 else '直线飞行'}")
                        st.rerun()
                    else:
                        st.error("请先设置起点和终点")
            with col_btn2:
                if st.button("⏹️ 停止飞行", use_container_width=True):
                    st.session_state.simulation_running = False
                    st.session_state.heartbeat_sim.simulating = False
                    st.session_state.comm_sim.send_message("GCS", "OBC", "STOP_MISSION", "用户停止飞行")
                    st.info("飞行已停止")
                    st.rerun()

        st.markdown("**📊 数据导出**")
        col_exp1, col_exp2 = st.columns(2)
        with col_exp1:
            if st.button("📊 导出飞行数据", use_container_width=True):
                df = st.session_state.heartbeat_sim.export_flight_data()
                if not df.empty:
                    csv = df.to_csv(index=False)
                    st.download_button(label="📥 下载CSV", data=csv,
                                       file_name=f"flight_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                       mime="text/csv")
        with col_exp2:
            if st.button("📊 导出航点数据", use_container_width=True) and st.session_state.planned_path:
                waypoint_data = [{"航点序号": i+1, "航点类型": "起点" if i==0 else "终点" if i==len(st.session_state.planned_path)-1 else f"绕行点{i}",
                                 "经度": wp[0], "纬度": wp[1]} for i, wp in enumerate(st.session_state.planned_path)]
                csv = pd.DataFrame(waypoint_data).to_csv(index=False, encoding='utf-8-sig')
                st.download_button(label="📥 下载航点CSV", data=csv,
                                   file_name=f"waypoints_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                   mime="text/csv")

        st.markdown("---")
        st.markdown("### 📈 实时数据图表")
        c_ch1, c_ch2 = st.columns(2)
        with c_ch1:
            st.subheader("📊 速度 vs 时间")
            if len(st.session_state.heartbeat_sim.history) > 1:
                speed_data = [{"时间(s)": i * config.HEARTBEAT_INTERVAL, "速度(m/s)": h.speed}
                              for i, h in enumerate(st.session_state.heartbeat_sim.history[:30])]
                st.line_chart(pd.DataFrame(speed_data), x="时间(s)", y="速度(m/s)")
        with c_ch2:
            st.subheader("📏 剩余距离 vs 时间")
            if len(st.session_state.heartbeat_sim.history) > 1:
                dist_data = [{"时间(s)": i * config.HEARTBEAT_INTERVAL, "剩余距离(m)": max(0, h.remaining_distance)}
                             for i, h in enumerate(st.session_state.heartbeat_sim.history[:30])]
                st.line_chart(pd.DataFrame(dist_data), x="时间(s)", y="剩余距离(m)")

        c_ch3, c_ch4 = st.columns(2)
        with c_ch3:
            st.subheader("🔋 电量模拟 vs 时间")
            if len(st.session_state.heartbeat_sim.history) > 1:
                battery_data = []
                for i, h in enumerate(st.session_state.heartbeat_sim.history[:30]):
                    hist_battery = max(0, min(100, (1 - h.flight_time / 1800) * 100))
                    if h.voltage:
                        hist_voltage_pct = ((h.voltage - 21.0) / (22.2 - 21.0)) * 100
                        hist_battery = max(0, min(100, (hist_battery + hist_voltage_pct) / 2))
                    battery_data.append({"时间(s)": i * config.HEARTBEAT_INTERVAL, "电量(%)": hist_battery})
                st.line_chart(pd.DataFrame(battery_data), x="时间(s)", y="电量(%)")
            st.caption("💡 电量基于电压和飞行时间综合计算")
        with c_ch4:
            st.subheader("🎯 航点进度")
            if len(st.session_state.heartbeat_sim.history) > 1 and total_waypoints > 0:
                waypoint_data = []
                for i, h in enumerate(st.session_state.heartbeat_sim.history[:30]):
                    if h.arrived:
                        hist_waypoint = total_waypoints
                    else:
                        # 使用修正后的航点计算逻辑
                        segment_count = total_waypoints - 1
                        if segment_count > 0 and h.progress >= 0:
                            segment_index = int(h.progress * segment_count)
                            segment_index = min(segment_index, segment_count - 1) if segment_count > 0 else 0
                            if h.progress >= 0.999:
                                hist_waypoint = total_waypoints
                            else:
                                hist_waypoint = segment_index + 1
                            hist_waypoint = min(hist_waypoint, total_waypoints)
                            hist_waypoint = max(hist_waypoint, 1)
                        else:
                            hist_waypoint = 1
                    waypoint_data.append({"时间(s)": i * config.HEARTBEAT_INTERVAL, "已完成航点": hist_waypoint})
                st.line_chart(pd.DataFrame(waypoint_data), x="时间(s)", y="已完成航点")

        st.markdown("---")
        st.markdown("### 📋 飞行日志记录")
        display_flight_history()
    else:
        st.info("⏳ 等待心跳数据... 请在「航线规划」页面点击「开始飞行」")
        col_tip1, col_tip2, col_tip3 = st.columns(3)
        with col_tip1:
            st.info("💡 提示1：先在航线规划页面设置起点和终点")
        with col_tip2:
            st.info("💡 提示2：设置飞行高度和速度系数")
        with col_tip3:
            st.info("💡 提示3：点击「开始飞行」按钮启动模拟")

    if st.session_state.planned_path and len(st.session_state.planned_path) > 1:
        st.markdown("---")
        st.subheader("🗺️ 规划航线预览")
        st.success(f"📌 已规划 {len(st.session_state.planned_path)} 个航点（包括起点和终点），点击开始飞行后将按此航线飞行")
        with st.expander("📋 查看详细航点列表"):
            waypoint_table = [{"序号": i+1, "类型": "🚁 起点" if i==0 else "🏁 终点" if i==len(st.session_state.planned_path)-1 else f"📍 绕行点 {i}",
                              "经度": f"{wp[0]:.6f}", "纬度": f"{wp[1]:.6f}"} for i, wp in enumerate(st.session_state.planned_path)]
            st.table(pd.DataFrame(waypoint_table))
    
    # 自动刷新逻辑（放在函数末尾）
    if auto_refresh and st.session_state.simulation_running:
        import time
        time.sleep(2)  # 等待2秒
        st.rerun()


def display_monitor_map(flight_alt: float, latest):
    tiles = config.GAODE_SATELLITE_URL
    m = folium.Map(location=[latest.lat, latest.lng], zoom_start=18, tiles=tiles, attr="高德卫星地图")

    for obs in st.session_state.obstacles_gcj:
        coords = obs.get('polygon', [])
        height = obs.get('height', 30)
        if coords and len(coords) >= 3:
            color = "red" if height > flight_alt else "orange"
            folium.Polygon([[c[1], c[0]] for c in coords], color=color, weight=2, fill=True,
                          fill_opacity=0.3, popup=f"🚧 {obs.get('name')}\n高度: {height}m").add_to(m)

    if st.session_state.planned_path and len(st.session_state.planned_path) > 1:
        line_color = "purple" if "向左" in st.session_state.current_direction else "orange" if "向右" in st.session_state.current_direction else "green"
        folium.PolyLine([[p[1], p[0]] for p in st.session_state.planned_path], color=line_color,
                       weight=3, opacity=0.7, popup=f"规划航线 - {st.session_state.current_direction}").add_to(m)

    folium.Circle(radius=st.session_state.safety_radius, location=[latest.lat, latest.lng],
                 color="blue", weight=2, fill=True, fill_color="blue", fill_opacity=0.2,
                 popup=f"🛡️ 安全半径: {st.session_state.safety_radius}米").add_to(m)

    trail = [[hb.lat, hb.lng] for hb in st.session_state.heartbeat_sim.history[:50] if hb.lat and hb.lng]
    if len(trail) > 1:
        folium.PolyLine(trail, color="orange", weight=2, opacity=0.6, popup="历史飞行轨迹").add_to(m)

    folium.Marker([latest.lat, latest.lng], popup=f"当前位置\n高度: {latest.altitude}m\n速度: {latest.speed}m/s",
                 icon=folium.Icon(color='red', icon='plane', prefix='fa')).add_to(m)

    if st.session_state.points_gcj['A']:
        folium.Marker([st.session_state.points_gcj['A'][1], st.session_state.points_gcj['A'][0]], popup="起点 A",
                     icon=folium.Icon(color='green', icon='play', prefix='fa')).add_to(m)
    if st.session_state.points_gcj['B']:
        folium.Marker([st.session_state.points_gcj['B'][1], st.session_state.points_gcj['B'][0]], popup="终点 B",
                     icon=folium.Icon(color='red', icon='flag-checkered', prefix='fa')).add_to(m)

    if st.session_state.planned_path and len(st.session_state.planned_path) > 2:
        for i, point in enumerate(st.session_state.planned_path[1:-1]):
            folium.CircleMarker([point[1], point[0]], radius=4, color="yellow", fill=True,
                               fill_color="yellow", fill_opacity=0.8, popup=f"航点 {i+1}").add_to(m)

    folium_static(m, width=900, height=500)


def display_flight_history():
    df = st.session_state.heartbeat_sim.export_flight_data()
    if not df.empty:
        display_cols = ['timestamp', 'flight_time', 'lat', 'lng', 'altitude', 'speed', 'voltage', 'satellites', 'remaining_distance']
        display_cols = [c for c in display_cols if c in df.columns]
        rename = {'timestamp': '时间', 'flight_time': '飞行时间(s)', 'lat': '纬度', 'lng': '经度',
                  'altitude': '高度(m)', 'speed': '速度(m/s)', 'voltage': '电压(V)', 'satellites': '卫星数', 'remaining_distance': '剩余距离(m)'}
        st.dataframe(df[display_cols].head(10).rename(columns=rename), use_container_width=True)
    else:
        st.info("暂无飞行数据")


def update_flight_simulation():
    if st.session_state.simulation_running:
        if time.time() - st.session_state.last_hb_time >= config.HEARTBEAT_INTERVAL:
            try:
                new_hb = st.session_state.heartbeat_sim.update_and_generate(st.session_state.obstacles_gcj, st.session_state.comm_sim)
                if new_hb:
                    st.session_state.last_hb_time = time.time()
                    st.session_state.flight_history.append([new_hb.lng, new_hb.lat])
                    if len(st.session_state.flight_history) > 200:
                        st.session_state.flight_history.pop(0)
                    if not st.session_state.heartbeat_sim.simulating:
                        st.session_state.simulation_running = False
                        st.success("🏁 无人机已安全到达目的地！")
                        st.rerun()
            except Exception as e:
                st.error(f"更新心跳时出错: {e}")
        else:
            # 修复：避免无限递归，只在需要时更新时间戳
            if time.time() - st.session_state.last_hb_time < config.HEARTBEAT_INTERVAL:
                pass  # 正常情况，不需要更新时间戳

# ==================== 障碍物管理页面 ====================
def render_obstacle_management_page(flight_alt: float):
    """障碍物管理页面 - 专业地面站风格"""
    st.header("🚧 障碍物管理")

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    border-radius: 12px; padding: 15px; text-align: center; color: white;">
            <div style="font-size: 28px; font-weight: bold;">{len(st.session_state.obstacles_gcj)}</div>
            <div style="font-size: 12px; opacity: 0.9;">📊 障碍物总数</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        high_obs = sum(1 for obs in st.session_state.obstacles_gcj if obs.get('height', 30) > flight_alt)
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
                    border-radius: 12px; padding: 15px; text-align: center; color: white;">
            <div style="font-size: 28px; font-weight: bold;">{high_obs}</div>
            <div style="font-size: 12px; opacity: 0.9;">🔴 需避让障碍物</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        safe_obs = len(st.session_state.obstacles_gcj) - high_obs
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
                    border-radius: 12px; padding: 15px; text-align: center; color: white;">
            <div style="font-size: 28px; font-weight: bold;">{safe_obs}</div>
            <div style="font-size: 12px; opacity: 0.9;">🟠 安全障碍物</div>
        </div>
        """, unsafe_allow_html=True)
    with col4:
        total_vertices = sum(len(obs.get('polygon', [])) for obs in st.session_state.obstacles_gcj)
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #43e97b 0%, #38f9d7 100%);
                    border-radius: 12px; padding: 15px; text-align: center; color: white;">
            <div style="font-size: 28px; font-weight: bold;">{total_vertices}</div>
            <div style="font-size: 12px; opacity: 0.9;">📍 总顶点数</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("### 🛠️ 工具栏")
    tool_cols = st.columns([1, 1, 1, 1, 2])
    with tool_cols[0]:
        if st.button("💾 保存配置", use_container_width=True, type="primary"):
            if save_obstacles(st.session_state.obstacles_gcj):
                st.success(f"✅ 已保存 {len(st.session_state.obstacles_gcj)} 个障碍物")
                st.balloons()
    with tool_cols[1]:
        if st.session_state.obstacles_gcj:
            config_data = {
                'obstacles': st.session_state.obstacles_gcj,
                'count': len(st.session_state.obstacles_gcj),
                'export_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'version': 'v13.2'
            }
            st.download_button(label="📥 导出配置", data=json.dumps(config_data, ensure_ascii=False, indent=2),
                               file_name=f"obstacles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                               mime="application/json", use_container_width=True)
        else:
            st.download_button(label="📥 导出配置", data=json.dumps({"obstacles": [], "count": 0}, ensure_ascii=False, indent=2),
                               file_name=f"obstacles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                               mime="application/json", use_container_width=True, disabled=True)
            st.caption("📭 暂无障碍物")
    with tool_cols[2]:
        latest_backup = get_latest_backup()
        if latest_backup:
            if st.button("🔄 恢复备份", use_container_width=True):
                if restore_from_backup(latest_backup):
                    st.session_state.obstacles_gcj = load_obstacles()
                    for obs in st.session_state.obstacles_gcj:
                        obs['selected'] = False
                    update_path_after_obstacle_change(flight_alt)
                    st.success("✅ 已从备份恢复")
                    st.rerun()
                else:
                    st.error("❌ 恢复失败")
        else:
            st.button("🔄 恢复备份", use_container_width=True, disabled=True)
            st.caption("📭 暂无备份")
    with tool_cols[3]:
        if st.button("🗑️ 清除全部", use_container_width=True):
            if st.session_state.obstacles_gcj:
                if st.session_state.auto_backup:
                    backup_config()
                st.session_state.obstacles_gcj = []
                save_obstacles([])
                update_path_after_obstacle_change(flight_alt)
                st.success("✅ 已清除所有障碍物")
                st.rerun()
            else:
                st.warning("⚠️ 无障碍物")
    with tool_cols[4]:
        if os.path.exists(config.CONFIG_FILE):
            try:
                with open(config.CONFIG_FILE, 'r', encoding='utf-8') as f:
                    save_time = json.load(f).get('save_time', '未知')
                st.info(f"💾 最后保存: {save_time}")
            except:
                st.info("💾 未保存")
        else:
            st.info("💾 未保存")

    st.markdown("---")
    info_cols = st.columns(3)
    with info_cols[0]:
        avg_height = sum(obs.get('height', 30) for obs in st.session_state.obstacles_gcj) / max(1, len(st.session_state.obstacles_gcj))
        st.metric("📏 平均高度", f"{avg_height:.1f} m")
    with info_cols[1]:
        backup_count = len([f for f in os.listdir(config.BACKUP_DIR) if f.startswith(config.CONFIG_FILE) and f.endswith('.bak')])
        st.metric("📦 备份数量", backup_count)
    with info_cols[2]:
        st.metric("🛡️ 安全半径", f"{st.session_state.safety_radius} 米")

    st.markdown("---")
    with st.expander("🎯 批量操作", expanded=False):
        for obs in st.session_state.obstacles_gcj:
            if 'selected' not in obs:
                obs['selected'] = False
        
        batch_cols = st.columns([1, 1, 1, 2])
        with batch_cols[0]:
            select_all = st.checkbox("☑️ 全选", key="select_all_obs")
            if select_all:
                for obs in st.session_state.obstacles_gcj:
                    obs['selected'] = True
        with batch_cols[1]:
            if st.button("🗑️ 批量删除", use_container_width=True, type="primary"):
                selected = [i for i, obs in enumerate(st.session_state.obstacles_gcj) if obs.get('selected', False)]
                if selected:
                    if st.session_state.auto_backup:
                        backup_config()
                    for i in reversed(selected):
                        st.session_state.obstacles_gcj.pop(i)
                    save_obstacles(st.session_state.obstacles_gcj)
                    update_path_after_obstacle_change(flight_alt)
                    st.success(f"✅ 已删除 {len(selected)} 个障碍物")
                    st.rerun()
                else:
                    st.warning("⚠️ 请先选择障碍物")
        with batch_cols[2]:
            batch_height = st.number_input("批量高度(m)", 1, 200, 30, 5, key="batch_height", label_visibility="collapsed")
            if st.button("📏 批量设置", use_container_width=True):
                selected = [i for i, obs in enumerate(st.session_state.obstacles_gcj) if obs.get('selected', False)]
                if selected:
                    for i in selected:
                        st.session_state.obstacles_gcj[i]['height'] = batch_height
                    if st.session_state.auto_backup:
                        save_obstacles(st.session_state.obstacles_gcj)
                    update_path_after_obstacle_change(flight_alt)
                    st.success(f"✅ 已设置 {len(selected)} 个障碍物")
                    st.rerun()
                else:
                    st.warning("⚠️ 请先选择障碍物")
        with batch_cols[3]:
            if st.button("🏷️ 批量重命名", use_container_width=True):
                selected = [i for i, obs in enumerate(st.session_state.obstacles_gcj) if obs.get('selected', False)]
                if selected:
                    st.session_state.show_rename_dialog = True
                else:
                    st.warning("⚠️ 请先选择障碍物")
        
        if st.session_state.get('show_rename_dialog', False):
            with st.container():
                st.markdown("---")
                st.markdown("#### 🏷️ 批量重命名")
                rename_cols = st.columns([1, 1, 1, 1])
                with rename_cols[0]:
                    name_prefix = st.text_input("前缀", value="建筑物")
                with rename_cols[1]:
                    start_number = st.number_input("起始编号", 1, 100, 1)
                with rename_cols[2]:
                    name_suffix = st.text_input("后缀", value="")
                with rename_cols[3]:
                    col_confirm, col_cancel = st.columns(2)
                    with col_confirm:
                        if st.button("确认", use_container_width=True, type="primary"):
                            selected = [i for i, obs in enumerate(st.session_state.obstacles_gcj) if obs.get('selected', False)]
                            for idx, i in enumerate(selected):
                                st.session_state.obstacles_gcj[i]['name'] = f"{name_prefix}{start_number + idx}{name_suffix}"
                            if st.session_state.auto_backup:
                                save_obstacles(st.session_state.obstacles_gcj)
                            st.session_state.show_rename_dialog = False
                            st.success(f"✅ 已重命名 {len(selected)} 个障碍物")
                            st.rerun()
                    with col_cancel:
                        if st.button("取消", use_container_width=True):
                            st.session_state.show_rename_dialog = False
                            st.rerun()

    st.markdown("---")
    tab1, tab2 = st.tabs(["📋 列表视图", "🗺️ 地图视图"])
    with tab1:
        render_obstacle_list_view(flight_alt)
    with tab2:
        render_obstacle_map_view(flight_alt)

def render_obstacle_list_view(flight_alt: float):
    st.subheader("📝 障碍物列表")
    st.caption("💡 提示：勾选复选框后可使用批量操作功能")
    
    if not st.session_state.obstacles_gcj:
        st.info("📭 暂无任何障碍物，可以在「地图视图」中绘制添加")
        return
    
    items_per_row = 3
    rows = (len(st.session_state.obstacles_gcj) + items_per_row - 1) // items_per_row
    
    for row in range(rows):
        cols = st.columns(items_per_row)
        for col_idx in range(items_per_row):
            idx = row * items_per_row + col_idx
            if idx < len(st.session_state.obstacles_gcj):
                render_obstacle_card(idx, flight_alt, cols[col_idx])

def render_obstacle_card(idx: int, flight_alt: float, container):
    obs = st.session_state.obstacles_gcj[idx]
    with container:
        with st.container(border=True):
            height = obs.get('height', 30)
            color = "🔴" if height > flight_alt else "🟠"
            name = obs.get('name', f'障碍物{idx+1}')
            
            header_cols = st.columns([1, 5])
            with header_cols[0]:
                checked = st.checkbox("", key=f"select_card_{idx}", value=obs.get('selected', False))
                st.session_state.obstacles_gcj[idx]['selected'] = checked
            with header_cols[1]:
                st.markdown(f"**{color} {name}**")
            
            info_cols = st.columns(2)
            with info_cols[0]:
                st.caption(f"📏 高度: {height}m")
            with info_cols[1]:
                st.caption(f"📍 顶点: {len(obs.get('polygon', []))}个")
            
            new_h = st.number_input("高度", min_value=1, max_value=200, value=height, step=5,
                                    key=f"quick_edit_{idx}", label_visibility="collapsed")
            if new_h != height:
                obs['height'] = new_h
                if st.session_state.auto_backup:
                    save_obstacles(st.session_state.obstacles_gcj)
                update_path_after_obstacle_change(flight_alt)
                st.rerun()
            
            if st.button("🗑️ 删除", key=f"delete_card_{idx}", use_container_width=True):
                if st.session_state.auto_backup:
                    backup_config()
                st.session_state.obstacles_gcj.pop(idx)
                save_obstacles(st.session_state.obstacles_gcj)
                update_path_after_obstacle_change(flight_alt)
                st.success(f"✅ 已删除 {name}")
                st.rerun()

def render_obstacle_map_view(flight_alt: float):
    st.subheader("🗺️ 地图视图")
    st.caption("✏️ 使用左上角绘制工具绘制新障碍物 | 🖱️ 点击障碍物查看详细信息 | 🎨 红色=需避让，橙色=安全")
    
    tiles = config.GAODE_SATELLITE_URL
    m = folium.Map(location=[config.SCHOOL_CENTER_GCJ[1], config.SCHOOL_CENTER_GCJ[0]], zoom_start=16, tiles=tiles, attr="高德卫星地图")
    
    draw = plugins.Draw(
        export=True, position='topleft',
        draw_options={
            'polygon': {'allowIntersection': False, 'showArea': True, 'color': '#ff0000',
                        'fillColor': '#ff0000', 'fillOpacity': 0.4},
            'polyline': False, 'rectangle': False, 'circle': False, 'marker': False, 'circlemarker': False
        },
        edit_options={'edit': True, 'remove': True}
    )
    m.add_child(draw)
    
    for obs in st.session_state.obstacles_gcj:
        coords = obs.get('polygon', [])
        height = obs.get('height', 30)
        color = "red" if height > flight_alt else "orange"
        if coords and len(coords) >= 3:
            popup_text = f"""
            <div style="font-family: sans-serif; min-width: 150px;">
                <b>🏢 {obs.get('name', '未知')}</b><br>
                📏 高度: {height} 米<br>
                📍 顶点: {len(coords)} 个<br>
                🆔 ID: {obs.get('id', 'N/A')[:12]}
            </div>
            """
            folium.Polygon([[c[1], c[0]] for c in coords], color=color, weight=3, fill=True,
                          fill_color=color, fill_opacity=0.5, popup=folium.Popup(popup_text, max_width=250)).add_to(m)
    
    folium.Marker([config.DEFAULT_A_GCJ[1], config.DEFAULT_A_GCJ[0]], popup="🟢 起点 (默认)",
                 icon=folium.Icon(color='green', icon='play', prefix='fa')).add_to(m)
    folium.Marker([config.DEFAULT_B_GCJ[1], config.DEFAULT_B_GCJ[0]], popup="🔴 终点 (默认)",
                 icon=folium.Icon(color='red', icon='flag-checkered', prefix='fa')).add_to(m)
    
    output = st_folium(m, width=850, height=550, key="obstacle_map_view", returned_objects=["last_active_drawing"])
    
    if output and output.get("last_active_drawing"):
        last = output["last_active_drawing"]
        if last and last.get("geometry") and last["geometry"].get("type") == "Polygon":
            coords = last["geometry"].get("coordinates", [[]])[0]
            poly = [[p[0], p[1]] for p in coords]
            if len(poly) >= 3 and st.session_state.pending_obstacle is None and validate_polygon(poly):
                st.session_state.pending_obstacle = poly
                st.rerun()
    
    if st.session_state.pending_obstacle is not None:
        render_obstacle_dialog()

def render_obstacle_dialog():
    """添加障碍物对话框"""
    st.markdown("---")
    st.subheader("📝 添加新障碍物")
    
    info_cols = st.columns(2)
    with info_cols[0]:
        st.info(f"📐 检测到多边形，共 {len(st.session_state.pending_obstacle)} 个顶点")
    with info_cols[1]:
        st.info(f"🖱️ 双击完成绘制，点击确认添加")
    
    col1, col2 = st.columns(2)
    with col1:
        new_name = st.text_input("障碍物名称", value=f"建筑物{len(st.session_state.obstacles_gcj) + 1}",
                                 help="为障碍物设置一个便于识别的名称")
    with col2:
        new_height = st.number_input("障碍物高度 (米)", min_value=1, max_value=200, value=30, step=5,
                                     key="height_input", help="低于飞行高度的障碍物不会触发避让")
    
    col_ok, col_cancel = st.columns(2)
    with col_ok:
        if st.button("✅ 确认添加", use_container_width=True, type="primary"):
            new_obstacle = {
                "name": new_name,
                "polygon": st.session_state.pending_obstacle,
                "height": new_height,
                "selected": False,
                "id": f"obs_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{len(st.session_state.obstacles_gcj)}",
                "created_time": datetime.now().isoformat()
            }
            st.session_state.obstacles_gcj.append(new_obstacle)
            if st.session_state.auto_backup:
                save_obstacles(st.session_state.obstacles_gcj)
            st.session_state.planned_path = create_avoidance_path(
                st.session_state.points_gcj['A'], st.session_state.points_gcj['B'],
                st.session_state.obstacles_gcj, st.session_state.last_flight_altitude,
                st.session_state.current_direction, st.session_state.safety_radius)
            st.session_state.pending_obstacle = None
            st.success(f"✅ 已添加 {new_name}，高度 {new_height} 米")
            st.rerun()
    with col_cancel:
        if st.button("❌ 取消", use_container_width=True):
            st.session_state.pending_obstacle = None
            st.rerun()

def update_path_after_obstacle_change(flight_alt: float):
    """障碍物变化后更新路径"""
    if st.session_state.points_gcj['A'] and st.session_state.points_gcj['B']:
        st.session_state.planned_path = create_avoidance_path(
            st.session_state.points_gcj['A'], st.session_state.points_gcj['B'],
            st.session_state.obstacles_gcj, flight_alt,
            st.session_state.current_direction, st.session_state.safety_radius)

# ==================== 主程序 ====================
def main():
    st.set_page_config(page_title="无人机地面站系统", layout="wide")
    init_session_state()
    st.title("🏫 无人机地面站系统")
    st.markdown("---")

    page, drone_speed, flight_alt, auto_save = render_sidebar()
    st.session_state.auto_backup = auto_save

    if flight_alt != st.session_state.last_flight_altitude:
        st.session_state.last_flight_altitude = flight_alt
        if st.session_state.planned_path:
            st.session_state.planned_path = create_avoidance_path(
                st.session_state.points_gcj['A'], st.session_state.points_gcj['B'],
                st.session_state.obstacles_gcj, flight_alt,
                st.session_state.current_direction, st.session_state.safety_radius)

    if page == "🗺️ 航线规划":
        render_planning_page(drone_speed, flight_alt, auto_save)
    elif page == "📡 飞行监控":
        render_flight_monitoring_page(flight_alt, drone_speed)
    elif page == "🔗 通信拓扑":
        render_communication_page()
    elif page == "🚧 障碍物管理":
        render_obstacle_management_page(flight_alt)

if __name__ == "__main__":
    main()
