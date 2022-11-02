python_sources(name="lib", dependencies=["./sql:models"])
python_tests(name="tests")

pex_binary(
    name="s2sf",
    entry_point="ingest.py",
)