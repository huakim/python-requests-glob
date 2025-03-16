def test_glob():
    from requests_glob import GlobAdapter
    from requests import Session
    import os

    s = Session()
    s.mount("glob://", GlobAdapter(netloc_paths={".": os.getcwd()}))
    f = s.get("glob://./globtext*.txt")
    assert f.text != ""
    assert f.text == "text\n"
