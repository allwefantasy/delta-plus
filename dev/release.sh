mvn clean deploy -DskipTests -Pdisable-java8-doclint -Prelease-sign-artifacts -Pscala-2.11

./dev/change-scala-version 2.12
mvn clean deploy -DskipTests -Pdisable-java8-doclint -Prelease-sign-artifacts -Pscala-2.12 -Pspark-3.0
git co .

mvn clean install -DskipTests -Pdisable-java8-doclint -Prelease-sign-artifacts
