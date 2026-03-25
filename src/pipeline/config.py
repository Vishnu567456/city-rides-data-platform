from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    project_root: Path
    data_dir: Path
    bronze_dir: Path
    silver_dir: Path
    gold_dir: Path
    monitoring_dir: Path
    warehouse_path: Path
    quality_report_path: Path


def resolve_paths(project_root: Path | None = None) -> Paths:
    root = project_root or Path(__file__).resolve().parents[2]
    data_dir = root / "data"
    return Paths(
        project_root=root,
        data_dir=data_dir,
        bronze_dir=data_dir / "bronze" / "trips",
        silver_dir=data_dir / "silver",
        gold_dir=data_dir / "gold",
        monitoring_dir=data_dir / "monitoring",
        warehouse_path=data_dir / "warehouse.duckdb",
        quality_report_path=data_dir / "quality_report.json",
    )
