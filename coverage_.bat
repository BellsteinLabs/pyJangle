@ECHO OFF
ECHO TEST
coverage run -m unittest discover -v -s ./pyjangle -p *_test.py
coverage lcov -o coverage.xml