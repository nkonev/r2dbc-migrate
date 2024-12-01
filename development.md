# Making a release
```bash
export JAVA_HOME=/usr/lib/jvm/java-17
./mvnw clean
./mvnw -Dresume=false -DskipTests release:prepare release:perform
git fetch
```

# FAQ:
## Q: Tests are failed with `com.github.dockerjava.api.exception.NotFoundException` on Linux Desktop (Fedora 35), but works on Github Actions
but user is in docker group
```
[nkonev@fedora ~]$ groups
nkonev wheel docker
```
`com.github.dockerjava.api.exception.NotFoundException: Status 404: {"message":"No such container: e24bc989de97049777f05be9f3a7cebf6389ddc349a91ea13f413a599ac4a8e1"}`
Also this is mentioned [here](https://github.com/testcontainers/testcontainers-java/issues/572#issuecomment-411703450).
A: 
```
su -
setenforce 0
```
