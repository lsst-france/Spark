Essai d'implémtation d'un serveur WEB en Scala/Spark

tout est dans ./web/src/...

* voir le fichier web/build.sbt

* lancer le serveur par :

   $HOME/sbt run
   
* tester par curl:

    curl -i http://localhost:8080/hello/world
    curl -i http://localhost:8080/hello/xxx
    curl -i http://localhost:8080/yyy/xxx
    
Ce développement est basé sur le package http://http4s.org/

