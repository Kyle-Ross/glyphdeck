@echo off
cd ..\docs && make clean html && make html && cd _build\html && start python -m http.server && start http://localhost:8000