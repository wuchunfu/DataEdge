# Compiling and Building seatunnel Source Code

## How to Build Binary Release Package

```bash
mvn clean package -Dmaven.test.skip=true
```

After above command finish, you will see the SeaTunnel distribution source package `apache-seatunnel-incubating-${version}-src.tar.gz` and the binary package `apache-seatunnel-incubating-${version}-bin.tar.gz` in directory `seatunnel-dist/target/`.
