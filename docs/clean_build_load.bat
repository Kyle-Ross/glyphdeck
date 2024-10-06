@echo off
cd F:\Github\glyphdeck\docs && make clean html && make html && cd F:\Github\glyphdeck\docs\_build\html && start python -m http.server && start http://localhost:8000