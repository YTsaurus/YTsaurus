YA_MAKE=../../../../../ya make
FIO_SRC=${HOME}/src/fio
DOCKER_REGISTRY=cr.ai.nebius.cloud/crnf2coti090683j5ssi/junk/khlebnikov
DOCKER_IMAGE=yt-fio:latest

build-release:
	${YA_MAKE} --build=relwithdebinfo -DSTRIP=yes -DFIO_SRC=${FIO_SRC}

build-debug:
	${YA_MAKE} --build=debug -DFIO_SRC=${FIO_SRC}

docker-image:
	$(MAKE) build-release
	ls -Lhs libfio-ytsaurus.so
	cp -L libfio-ytsaurus.so ${FIO_SRC}/fio-ytsaurus.so
	docker buildx build -t ${DOCKER_REGISTRY}/${DOCKER_IMAGE} --push -f Dockerfile ${FIO_SRC}
