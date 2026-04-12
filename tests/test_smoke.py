import pytest

duckdb = pytest.importorskip("duckdb")
from pathlib import Path

from evalsuite.benchmarks.bird import BirdSQLiteBenchmark
from evalsuite.benchmarks.spider2 import Spider2Benchmark
from evalsuite.benchmarks.tpcds import TPCDSNLBenchmark as TPCDSBenchmark
from evalsuite.core.config import ComparatorConfig, Config, DatasetPaths, ModelConfig


class FakeModel:
    def generate_sql(self, question: str, schema: str | None = None) -> str:
        if "version" in question.lower():
            return "SELECT sqlite_version();"
        if "how many rows are in the sample" in question.lower():
            return "SELECT COUNT(*) FROM sample;"
        if "values from sample ordered" in question.lower():
            return "SELECT value FROM sample ORDER BY value ASC;"
        if "average v in metrics" in question.lower():
            return "SELECT AVG(v) FROM metrics;"
        if "sum of v" in question.lower():
            return "SELECT SUM(v) FROM metrics;"
        if "max v" in question.lower():
            return "SELECT MAX(v) FROM metrics;"
        if "min v" in question.lower():
            return "SELECT MIN(v) FROM metrics;"
        if "distinct values in sample" in question.lower():
            return "SELECT COUNT(DISTINCT value) FROM sample;"
        if "contains 'alpha'" in question.lower():
            return "SELECT COUNT(*) FROM sample WHERE value='alpha';"
        if "metrics greater than 10" in question.lower():
            return "SELECT name, v FROM metrics WHERE v > 10 ORDER BY v DESC;"
        if "metrics less than 25" in question.lower():
            return "SELECT name, v FROM metrics WHERE v < 25 ORDER BY v ASC;"
        if "average v by first letter" in question.lower():
            return "SELECT substr(name,1,1) AS prefix, AVG(v) FROM metrics GROUP BY prefix;"
        if "how many metrics entries" in question.lower():
            return "SELECT COUNT(*) FROM metrics;"
        if "top metric by v" in question.lower():
            return "SELECT name, v FROM metrics ORDER BY v DESC LIMIT 1;"
        if "bottom metric by v" in question.lower():
            return "SELECT name, v FROM metrics ORDER BY v ASC LIMIT 1;"
        if "with id > 1" in question.lower():
            return "SELECT value FROM sample WHERE id > 1 ORDER BY id;"
        if "with id < 3" in question.lower():
            return "SELECT value FROM sample WHERE id < 3 ORDER BY id;"
        if "ids reversed" in question.lower():
            return "SELECT id FROM sample ORDER BY id DESC;"
        if "join sample to metrics" in question.lower():
            return "SELECT s.id, s.value, m.name FROM sample s LEFT JOIN metrics m ON (s.id % 2) = (m.v % 2) ORDER BY s.id;"
        if "table name" in question.lower():
            return "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;"
        if "table" in question.lower():
            return "SELECT COUNT(*) FROM sqlite_master WHERE type='table';"
        if "total sales amount" in question.lower():
            return "SELECT sum(amount) FROM demo_sales;"
        if "top category" in question.lower():
            return "SELECT category, sum(amount) as total FROM demo_sales GROUP BY category ORDER BY total DESC;"
        if "how many demo_sales rows" in question.lower():
            return "SELECT count(*) FROM demo_sales;"
        if "average amount per category" in question.lower():
            return "SELECT category, avg(amount) FROM demo_sales GROUP BY category;"
        if "distinct categories" in question.lower():
            return "SELECT DISTINCT category FROM demo_sales ORDER BY category;"
        if "max sale amount" in question.lower():
            return "SELECT max(amount) FROM demo_sales;"
        if "min sale amount" in question.lower():
            return "SELECT min(amount) FROM demo_sales;"
        if "sum per category" in question.lower():
            return "SELECT category, sum(amount) FROM demo_sales GROUP BY category ORDER BY category;"
        if "avg per category ordered desc" in question.lower():
            return "SELECT category, avg(amount) FROM demo_sales GROUP BY category ORDER BY avg(amount) DESC, category ASC;"
        if "rows per category" in question.lower():
            return "SELECT category, count(*) FROM demo_sales GROUP BY category ORDER BY category;"
        if "top 2 rows by amount" in question.lower():
            return "SELECT * FROM demo_sales ORDER BY amount DESC LIMIT 2;"
        if "bottom 2 rows by amount" in question.lower():
            return "SELECT * FROM demo_sales ORDER BY amount ASC LIMIT 2;"
        if "median-ish via percentile" in question.lower():
            return "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY amount) FROM demo_sales;"
        if "amount > 15" in question.lower():
            return "SELECT * FROM demo_sales WHERE amount > 15 ORDER BY amount DESC;"
        if "amount < 25" in question.lower():
            return "SELECT * FROM demo_sales WHERE amount < 25 ORDER BY amount ASC;"
        if "share of total" in question.lower():
            return "SELECT category, sum(amount)/(SELECT sum(amount) FROM demo_sales) AS share FROM demo_sales GROUP BY category ORDER BY share DESC;"
        if "difference between max and min" in question.lower():
            return "SELECT max(amount) - min(amount) FROM demo_sales;"
        if "standard deviation of amount" in question.lower():
            return "SELECT stddev_samp(amount) FROM demo_sales;"
        if "variance of amount" in question.lower():
            return "SELECT var_samp(amount) FROM demo_sales;"
        if "unique amounts" in question.lower():
            return "SELECT COUNT(DISTINCT amount) FROM demo_sales;"
        if "how many employees" in question.lower():
            return "SELECT COUNT(*) FROM employees;"
        if "departments and headcount" in question.lower():
            return "SELECT dept, COUNT(*) AS c FROM employees GROUP BY dept ORDER BY c DESC;"
        if "average salary per department" in question.lower():
            return "SELECT dept, AVG(salary) FROM employees GROUP BY dept ORDER BY dept;"
        if "total salary payout" in question.lower():
            return "SELECT SUM(salary) FROM employees;"
        if "max salary overall" in question.lower():
            return "SELECT MAX(salary) FROM employees;"
        if "min salary overall" in question.lower():
            return "SELECT MIN(salary) FROM employees;"
        if "employees in eng department" in question.lower():
            return "SELECT COUNT(*) FROM employees WHERE dept='eng';"
        if "average salary in eng" in question.lower():
            return "SELECT AVG(salary) FROM employees WHERE dept='eng';"
        if "ordered by salary desc" in question.lower():
            return "SELECT name, salary FROM employees ORDER BY salary DESC;"
        if "top 2 salaries" in question.lower():
            return "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 2;"
        if "bottom 2 salaries" in question.lower():
            return "SELECT name, salary FROM employees ORDER BY salary ASC LIMIT 2;"
        if "distinct departments" in question.lower():
            return "SELECT DISTINCT dept FROM employees ORDER BY dept;"
        if "salary sum per department" in question.lower():
            return "SELECT dept, SUM(salary) FROM employees GROUP BY dept ORDER BY dept;"
        if "eng vs non-eng headcount" in question.lower():
            return (
                "SELECT CASE WHEN dept='eng' THEN 'eng' ELSE 'other' END AS grp, COUNT(*) FROM employees GROUP BY grp;"
            )
        if "range (max - min)" in question.lower() or "range" in question.lower():
            return "SELECT MAX(salary) - MIN(salary) FROM employees;"
        if "salary above 100k" in question.lower():
            return "SELECT name FROM employees WHERE salary > 100000 ORDER BY salary DESC;"
        if "salary below 95k" in question.lower():
            return "SELECT name FROM employees WHERE salary < 95000 ORDER BY salary ASC;"
        if "highest average salary" in question.lower():
            return "SELECT dept FROM employees GROUP BY dept ORDER BY AVG(salary) DESC LIMIT 1;"
        return "SELECT 1;"


def _config(tmp: Path) -> Config:
    return Config(
        model=ModelConfig(),
        comparator=ComparatorConfig(),
        datasets=DatasetPaths(
            bird_root=tmp / "bird",
            spider2_root=tmp / "spider2",
            tpcds_duckdb=tmp / "tpcds" / "duckdb.db",
        ),
        raw={},
    )


def test_smoke_tasks_have_meta_flag(tmp_path: Path):
    """Smoke tasks produced by _default_tasks must have meta['smoke'] = True."""
    cfg = _config(tmp_path)
    model = FakeModel()

    bird = BirdSQLiteBenchmark(config=cfg, model=model)
    bird_tasks = bird.discover_tasks()
    assert len(bird_tasks) >= 20
    assert all(t.meta.get("smoke") for t in bird_tasks)

    spider = Spider2Benchmark(config=cfg, model=model)
    spider_tasks = spider.discover_tasks()
    assert len(spider_tasks) >= 20
    assert all(t.meta.get("smoke") for t in spider_tasks)

    tpcds = TPCDSBenchmark(config=cfg, model=model)
    tpcds_tasks = tpcds.discover_tasks()
    assert len(tpcds_tasks) >= 20
    assert all(t.meta.get("smoke") for t in tpcds_tasks)


def test_smoke_results_have_extra_flag(tmp_path: Path):
    """Results from smoke tasks must carry extra['smoke'] = True."""
    cfg = _config(tmp_path)
    model = FakeModel()

    bird = BirdSQLiteBenchmark(config=cfg, model=model)
    bird_tasks = bird.discover_tasks()
    bird_results = [bird.run_task(t) for t in bird_tasks]
    assert all(r.extra.get("smoke") for r in bird_results)

    tpcds = TPCDSBenchmark(config=cfg, model=model)
    tpcds_tasks = tpcds.discover_tasks()
    tpcds_results = [tpcds.run_task(t) for t in tpcds_tasks]
    assert all(r.extra.get("smoke") for r in tpcds_results)


def test_smoke_excluded_from_summary(tmp_path: Path):
    """summarize() must exclude smoke-flagged results, yielding total=0."""
    cfg = _config(tmp_path)
    model = FakeModel()

    bird = BirdSQLiteBenchmark(config=cfg, model=model)
    bird_tasks = bird.discover_tasks()
    bird_results = [bird.run_task(t) for t in bird_tasks]
    bird_summary = bird.summarize(bird_results)
    assert bird_summary.total == 0

    spider = Spider2Benchmark(config=cfg, model=model)
    spider_tasks = spider.discover_tasks()
    spider_results = [spider.run_task(t) for t in spider_tasks]
    spider_summary = spider.summarize(spider_results)
    assert spider_summary.total == 0

    tpcds = TPCDSBenchmark(config=cfg, model=model)
    tpcds_tasks = tpcds.discover_tasks()
    tpcds_results = [tpcds.run_task(t) for t in tpcds_tasks]
    tpcds_summary = tpcds.summarize(tpcds_results)
    assert tpcds_summary.total == 0


def test_benches_smoke_execution_still_works(tmp_path: Path):
    """Smoke tasks still execute correctly (pipeline sanity check), even though excluded from summary."""
    cfg = _config(tmp_path)
    model = FakeModel()

    bird = BirdSQLiteBenchmark(config=cfg, model=model)
    bird_tasks = bird.discover_tasks()
    bird_results = [bird.run_task(t) for t in bird_tasks]
    # All smoke tasks should still produce match=True (FakeModel returns gold SQL)
    matching = [r for r in bird_results if r.match]
    assert len(matching) == len(bird_results)

    spider = Spider2Benchmark(config=cfg, model=model)
    spider_tasks = spider.discover_tasks()
    spider_results = [spider.run_task(t) for t in spider_tasks]
    matching = [r for r in spider_results if r.match]
    assert len(matching) == len(spider_results)

    tpcds = TPCDSBenchmark(config=cfg, model=model)
    tpcds_tasks = tpcds.discover_tasks()
    tpcds_results = [tpcds.run_task(t) for t in tpcds_tasks]
    matching = [r for r in tpcds_results if r.match]
    assert len(matching) == len(tpcds_results)
