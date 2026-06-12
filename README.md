Pour utiliser ce projet, il faut avoir java 17


Pour activer l'environnement virtuel:
```bash
source .venv/bin/activate
```
Pour se synchroniser avec le lockfile:
```bash
uv sync --upgrade
```

Pour spécifier la bonne version de Java (sous linux): 
```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
export SPARK_LOCAL_IP=127.0.0.1
```

