CREATE TABLE cell_towers
(
    radio String,
    mcc Int16,
    net Int16,
    area Int32,
    cell Int64,
    unit Int16,
    lon Float64,
    lat Float64,
    range Int32,
    samples Int32,
    changeable Int8,
    created DateTime,
    updated DateTime,
    averageSignal Int16,
    version UInt64 DEFAULT now()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(created)
ORDER BY (mcc, net, area, cell)
SETTINGS index_granularity = 8192;



SELECT 
	area
FROM (
	SELECT 
		area,
		count() AS cell_count
	FROM 
		cell_towers 
	WHERE 
		mcc = 250 AND radio != 'LTE'
	GROUP BY area
)
WHERE 
	cell_count > 2000;
-- Результат: 315 строк