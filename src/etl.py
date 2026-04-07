import geopandas as gpd
import pandas as pd
import logging
import unicodedata
import uuid
from sqlalchemy import text
from .config import DATA_DIR, MILANO_GRID_FILE, PROVINCES_FILE, ISTAT_FILE, TRAFFIC_PATTERN, MOBILITY_PATTERN, TARGET_CRS
from .database import get_sqlalchemy_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _upsert_traffic_dataframe(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0

    # Prevent ON CONFLICT cardinality errors if source has duplicate keys.
    df = df.drop_duplicates(subset=['datetime', 'cell_id', 'countrycode'], keep='last')

    tmp_table = f"tmp_fact_traffic_{uuid.uuid4().hex[:8]}"
    conn.execute(text(f"CREATE TEMP TABLE {tmp_table} (LIKE fact_traffic_milan INCLUDING DEFAULTS) ON COMMIT DROP"))
    df.to_sql(tmp_table, conn, if_exists='append', index=False, chunksize=1000)

    result = conn.execute(
        text(
            f"""
            INSERT INTO fact_traffic_milan (
                datetime, cell_id, countrycode, smsin, smsout, callin, callout, internet
            )
            SELECT
                datetime, cell_id, countrycode, smsin, smsout, callin, callout, internet
            FROM {tmp_table}
            ON CONFLICT (datetime, cell_id, countrycode)
            DO UPDATE SET
                smsin = EXCLUDED.smsin,
                smsout = EXCLUDED.smsout,
                callin = EXCLUDED.callin,
                callout = EXCLUDED.callout,
                internet = EXCLUDED.internet
            """
        )
    )
    return int(result.rowcount or 0)


def _upsert_mobility_dataframe(df: pd.DataFrame, conn) -> int:
    if df.empty:
        return 0

    # Keep one row per natural key before upsert.
    df = df.drop_duplicates(subset=['datetime', 'cell_id', 'provincia'], keep='last')

    tmp_table = f"tmp_fact_mobility_{uuid.uuid4().hex[:8]}"
    conn.execute(text(f"CREATE TEMP TABLE {tmp_table} (LIKE fact_mobility_provinces INCLUDING DEFAULTS) ON COMMIT DROP"))
    df.to_sql(tmp_table, conn, if_exists='append', index=False, chunksize=1000)

    result = conn.execute(
        text(
            f"""
            INSERT INTO fact_mobility_provinces (
                datetime, cell_id, provincia, cell2province, province2cell
            )
            SELECT
                datetime, cell_id, provincia, cell2province, province2cell
            FROM {tmp_table}
            ON CONFLICT (datetime, cell_id, provincia)
            DO UPDATE SET
                cell2province = EXCLUDED.cell2province,
                province2cell = EXCLUDED.province2cell
            """
        )
    )
    return int(result.rowcount or 0)


def _normalize_province_name(name: str) -> str:
    if not isinstance(name, str):
        return ""

    normalized = unicodedata.normalize("NFKD", name)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.replace("’", "'")
    normalized = normalized.replace("`", "'")
    normalized = normalized.replace("-", "-")
    normalized = " ".join(normalized.strip().split()).title()
    aliases = {
        "Valle D'Aosta/Vallee D'Aoste": "Aosta",
        "Valle D'Aosta/Vallée D'Aoste": "Aosta",
        "Valle D'Aosta": "Aosta",
        "Valle D'Aoste": "Aosta",
        "Monza E Della Brianza": "Monza e della Brianza",
        "Reggio Nell'Emilia": "Reggio nell'Emilia",
        "Reggio Di Calabria": "Reggio di Calabria",
        "Pesaro E Urbino": "Pesaro e Urbino",
        "Massa-Carrara": "Massa Carrara",
        "Forli'-Cesena": "Forli'-Cesena",
        "Forli-Cesena": "Forli'-Cesena",
        "Forli Cesena": "Forli'-Cesena",
        "Forlì-Cesena": "Forli'-Cesena",
        "Bolzano/Bozen": "Bolzano",
    }
    return aliases.get(normalized, normalized)


def _load_istat_population() -> pd.DataFrame:
    if not ISTAT_FILE.exists():
        logger.warning(f"ISTAT file not found: {ISTAT_FILE}")
        return pd.DataFrame(columns=['provincia_norm', 'population'])

    istat_df = pd.read_csv(ISTAT_FILE, usecols=['PROVINCIA', 'P1'])
    istat_df = istat_df.rename(columns={'PROVINCIA': 'provincia', 'P1': 'population'})
    istat_df['provincia'] = istat_df['provincia'].astype(str).str.strip()
    istat_df['population'] = pd.to_numeric(istat_df['population'], errors='coerce')
    istat_df = istat_df.dropna(subset=['provincia', 'population'])
    istat_df['population'] = istat_df['population'].astype(int)
    istat_df['provincia_norm'] = istat_df['provincia'].apply(_normalize_province_name)
    istat_df = istat_df.drop_duplicates(subset=['provincia_norm'], keep='first')
    return istat_df[['provincia_norm', 'population']]


def sync_province_population_from_istat(engine=None) -> int:
    engine = engine or get_sqlalchemy_engine()
    istat_population = _load_istat_population()

    if istat_population.empty:
        logger.warning("No ISTAT population data loaded; skipping population sync")
        return 0

    provinces_df = pd.read_sql("SELECT provincia, population FROM dim_provinces_it", engine)
    if provinces_df.empty:
        logger.info("No provinces in dim_provinces_it yet; skipping population sync")
        return 0

    provinces_df['provincia_norm'] = provinces_df['provincia'].apply(_normalize_province_name)
    merged = provinces_df.merge(
        istat_population,
        on='provincia_norm',
        how='left',
        suffixes=('_db', '_istat')
    )

    updates = merged.dropna(subset=['population_istat']).copy()
    updates['population_db'] = pd.to_numeric(updates['population_db'], errors='coerce').fillna(-1).astype(int)
    updates['population_istat'] = updates['population_istat'].astype(int)
    updates = updates[updates['population_db'] != updates['population_istat']]

    if updates.empty:
        logger.info("Province population already synchronized with ISTAT P1")
        return 0

    with engine.begin() as conn:
        for row in updates.itertuples(index=False):
            conn.execute(
                text(
                    """
                    UPDATE dim_provinces_it
                    SET population = :population
                    WHERE provincia = :provincia
                    """
                ),
                {'population': int(row.population_istat), 'provincia': row.provincia}
            )

    logger.info(f"✓ Updated {len(updates)} province populations from ISTAT P1")
    return int(len(updates))


def load_grid_geometries():
    try:
        logger.info(f"Loading {MILANO_GRID_FILE}")
        
        engine = get_sqlalchemy_engine()
        existing_count = pd.read_sql("SELECT COUNT(*) FROM dim_grid_milan", engine).iloc[0, 0]
        
        if existing_count > 0:
            logger.info(f"✓ {existing_count} grid cells already loaded (skipping new load)")
            # Backfill bounds if they are missing
            with engine.begin() as conn:
                conn.execute(text(
                    """
                    UPDATE dim_grid_milan
                    SET bounds = COALESCE(bounds, ST_AsText(ST_Envelope(geometry)))
                    WHERE bounds IS NULL
                    """
                ))
            return
        
        gdf = gpd.read_file(MILANO_GRID_FILE)
        
        if gdf.crs != TARGET_CRS:
            gdf = gdf.to_crs(TARGET_CRS)
        
        gdf['cell_id'] = gdf.index

        bounds_df = gdf.geometry.bounds
        gdf['bounds'] = bounds_df.apply(
            lambda row: f"{row.minx},{row.miny},{row.maxx},{row.maxy}", axis=1
        )
        
        gdf[['cell_id', 'geometry', 'bounds']].to_postgis(
            'dim_grid_milan',
            engine,
            if_exists='append',
            index=False
        )
        
        logger.info(f"✓ {len(gdf)} grid cells loaded")
        
    except Exception as e:
        logger.error(f"Grid loading error: {e}")
        raise


def load_provinces_geometries():
    try:
        logger.info(f"Loading {PROVINCES_FILE}")
        
        engine = get_sqlalchemy_engine()
        existing_count = pd.read_sql("SELECT COUNT(*) FROM dim_provinces_it", engine).iloc[0, 0]
        
        if existing_count > 0:
            logger.info(f"✓ {existing_count} provinces already loaded (skipping)")
            sync_province_population_from_istat(engine)
            return
        
        gdf = gpd.read_file(PROVINCES_FILE)
        
        if gdf.crs != TARGET_CRS:
            gdf = gdf.to_crs(TARGET_CRS)
        
        if 'PROVINCIA' in gdf.columns:
            gdf = gdf.rename(columns={'PROVINCIA': 'provincia'})
        elif 'name' in gdf.columns:
            gdf = gdf.rename(columns={'name': 'provincia'})

        if 'population' in gdf.columns:
            gdf['population'] = pd.to_numeric(gdf['population'], errors='coerce').fillna(0).astype(int)
        else:
            gdf['population'] = 0

        istat_population = _load_istat_population()
        if not istat_population.empty:
            gdf['provincia_norm'] = gdf['provincia'].apply(_normalize_province_name)
            gdf = gdf.merge(
                istat_population,
                on='provincia_norm',
                how='left',
                suffixes=('_geojson', '_istat')
            )
            gdf['population'] = (
                gdf['population_istat']
                .fillna(gdf['population_geojson'])
                .fillna(0)
                .astype(int)
            )
            gdf = gdf.drop(columns=['population_geojson', 'population_istat', 'provincia_norm'])
        else:
            logger.warning("Could not enrich provinces with ISTAT P1 population")
        
        gdf[['provincia', 'geometry', 'population']].to_postgis(
            'dim_provinces_it',
            engine,
            if_exists='append',
            index=False
        )
        
        logger.info(f"✓ {len(gdf)} provinces loaded")
        
    except Exception as e:
        logger.error(f"Provinces loading error: {e}")
        raise


def load_traffic_data(file_pattern=None, limit_files=None):
    try:
        engine = get_sqlalchemy_engine()

        pattern = file_pattern or TRAFFIC_PATTERN
        csv_files = sorted(DATA_DIR.glob(pattern))
        
        if limit_files:
            csv_files = csv_files[:limit_files]
        
        if not csv_files:
            logger.warning(f"No files found for pattern: {pattern}")
            return
        
        logger.info(f"Loading {len(csv_files)} traffic files...")
        
        total_rows = 0
        rejected_rows = []
        
        for csv_file in csv_files:
            logger.info(f"  - {csv_file.name}")
            df = pd.read_csv(csv_file)
            initial_count = len(df)
            invalid_dates = 0
            invalid_cells = 0
            
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                before = len(df)
                df = df.dropna(subset=['datetime'])
                invalid_dates = before - len(df)
                if invalid_dates:
                    logger.warning(f"    - {invalid_dates} invalid dates will be dropped")
            
            df = df.rename(columns={'CellID': 'cell_id'})

            metric_cols = ['smsin', 'smsout', 'callin', 'callout', 'internet']
            for col in metric_cols:
                if col not in df.columns:
                    df[col] = 0
                else:
                    neg_count = (df[col] < 0).sum()
                    if neg_count > 0:
                        logger.warning(f"    - {neg_count} negative values in {col}, setting to 0")
            df[metric_cols] = df[metric_cols].apply(pd.to_numeric, errors='coerce').fillna(0)
            for col in metric_cols:
                df.loc[df[col] < 0, col] = 0

            invalid_cells = df[~df['cell_id'].between(0, 9999)].shape[0]
            df = df[df['cell_id'].between(0, 9999)]

            final_count = len(df)
            rejected = initial_count - final_count
            if rejected:
                logger.info(f"    - Cleaned: {rejected} rows rejected ({final_count} kept)")
                rejected_rows.append({
                    'file': csv_file.name,
                    'initial': initial_count,
                    'final': final_count,
                    'rejected': rejected,
                    'invalid_dates': invalid_dates,
                    'invalid_cells': invalid_cells
                })
            
            with engine.begin() as conn:
                written = _upsert_traffic_dataframe(df, conn)
            total_rows += written
        
        logger.info(f"✓ {total_rows} traffic rows upserted from {len(csv_files)} files")
        if rejected_rows:
            total_rejected = sum(r['rejected'] for r in rejected_rows)
            logger.info(f"⚠ {total_rejected} total rows were rejected during cleaning")
        
    except Exception as e:
        logger.error(f"Traffic data loading error: {e}")
        raise


def load_mobility_data(file_pattern=None, limit_files=None):
    try:
        engine = get_sqlalchemy_engine()

        pattern = file_pattern or MOBILITY_PATTERN
        csv_files = sorted(DATA_DIR.glob(pattern))
        
        if limit_files:
            csv_files = csv_files[:limit_files]
        
        if not csv_files:
            logger.warning(f"No files found for pattern: {pattern}")
            return
        
        logger.info(f"Loading {len(csv_files)} mobility files...")
        
        total_rows = 0
        
        province_map = {
            "Monza E Della Brianza": "Monza e della Brianza",
            "Reggio Nell'Emilia": "Reggio nell'Emilia",
            "Reggio Di Calabria": "Reggio di Calabria",
            "Pesaro E Urbino": "Pesaro e Urbino",
            "Massa-Carrara": "Massa Carrara",
            "Valle D'Aosta": "Aosta",
            "Bolzano/Bozen": "Bolzano",
        }

        valid_provinces = pd.read_sql(
            "SELECT provincia FROM dim_provinces_it",
            engine
        )['provincia']

        for csv_file in csv_files:
            logger.info(f"  - {csv_file.name}")
            df = pd.read_csv(csv_file)
            
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
                before = len(df)
                df = df.dropna(subset=['datetime'])
                dropped_null = before - len(df)
                if dropped_null:
                    logger.info(f"    - dropped {dropped_null} rows with null/invalid datetime from {csv_file.name}")
            
            df = df.rename(columns={
                'CellID': 'cell_id',
                'provinceName': 'provincia',
                'cell2Province': 'cell2province',
                'Province2cell': 'province2cell'
            })

            for col in ['cell2province', 'province2cell']:
                if col not in df.columns:
                    df[col] = 0
            df[['cell2province', 'province2cell']] = df[['cell2province', 'province2cell']].apply(pd.to_numeric, errors='coerce').fillna(0)

            if 'provincia' in df.columns:
                df['provincia'] = df['provincia'].str.title().str.strip()
                df['provincia'] = df['provincia'].replace(province_map)
                before = len(df)
                df = df[df['provincia'].isin(valid_provinces)]
                dropped = before - len(df)
                if dropped:
                    logger.info(f"    - dropped {dropped} rows with unmatched provinces from {csv_file.name}")
            
            df = df[df['cell_id'].between(0, 9999)]
            
            with engine.begin() as conn:
                written = _upsert_mobility_dataframe(df, conn)
            total_rows += written
        
        logger.info(f"✓ {total_rows} mobility rows upserted from {len(csv_files)} files")
        
    except Exception as e:
        logger.error(f"Mobility data loading error: {e}")
        raise


def get_top_cells(limit=10):
    query = f"""
    SELECT cell_id, AVG(total_activity) as avg_load 
    FROM v_hourly_traffic 
    WHERE hour >= '2013-11-01 00:00'::timestamptz
    GROUP BY cell_id 
    ORDER BY avg_load DESC 
    LIMIT {limit};
    """
    
    try:
        engine = get_sqlalchemy_engine()
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        logger.error(f"Query error: {e}")
        raise


def validate_schema_constraints(engine=None):
    """Check simple non-negative constraints at the DB level."""
    engine = engine or get_sqlalchemy_engine()
    constraints = [
        ("dim_grid_milan", "(cell_id BETWEEN 0 AND 9999)"),
        ("dim_provinces_it", "(population >= 0)"),
        ("fact_traffic_milan", "(smsin >= 0)"),
        ("fact_traffic_milan", "(smsout >= 0)"),
        ("fact_traffic_milan", "(callin >= 0)"),
        ("fact_traffic_milan", "(callout >= 0)"),
        ("fact_traffic_milan", "(internet >= 0)"),
        ("fact_mobility_provinces", "(cell2province >= 0)"),
        ("fact_mobility_provinces", "(province2cell >= 0)")
    ]

    for table, condition in constraints:
        query = f"""
        SELECT COUNT(*) AS violations
        FROM {table}
        WHERE NOT {condition}
        """
        df = pd.read_sql(query, engine)
        violations = int(df.iloc[0]['violations'])
        if violations > 0:
            logger.warning(f"⚠ {violations} violations in {table} for constraint {condition}")
        else:
            logger.info(f"✓ {table}: {condition} - No violations")
