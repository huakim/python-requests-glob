def test_glob():
  from requests_glob import GlobAdapter
  from requests import Session
  s = Session()
  s.mount('glob://', GlobAdapter())
  f=s.get('glob://./globtext*.txt')
  assert f.text != ''
  assert f.text == "text\n"
