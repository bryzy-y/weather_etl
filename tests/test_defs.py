from src.weather_etl.definitions import defs


def test_def_can_load():
    """Test that the definitions can be loaded without errors."""
    assert defs().resolve_all_job_defs()
