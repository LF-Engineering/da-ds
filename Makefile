GO_LIB_FILES=affs.go context.go const.go ds.go dsconfluence.go dsgerrit.go dsgit.go dsgroupsio.go dsjira.go dsrocketchat.go dsstub.go email.go es.go error.go exec.go json.go log.go mbox.go redacted.go sql.go threads.go time.go utils.go uuid.go api.go token.go
GO_BIN_FILES=cmd/dads/dads.go
GO_TEST_FILES=context_test.go email_test.go regexp_test.go time_test.go threads_test.go
GO_LIBTEST_FILES=test/time.go
GO_BIN_CMDS=github.com/LF-Engineering/da-ds/cmd/dads
# for race CGO_ENABLED=1
# GO_ENV=CGO_ENABLED=1
GO_ENV=CGO_ENABLED=0
# for race -race
# GO_BUILD=go build -ldflags '-s -w' -race
GO_BUILD=go build -ldflags '-s -w'
GO_INSTALL=go install -ldflags '-s'
GO_FMT=gofmt -s -w
GO_LINT=golint -set_exit_status
GO_VET=go vet
GO_IMPORTS=goimports -w
GO_USEDEXPORTS=usedexports
GO_ERRCHECK=errcheck -asserts -ignoretests -ignoregenerated
GO_TEST=go test
BINARIES=dads
STRIP=strip
PKG_LIST := $(shell go list ./... | grep -v mock)
PRODUCT_NAME?=da-ds
COMMIT=`git rev-parse --short HEAD`

all: check build

build: cmd/dads/dads.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o dads "-w -X github.com/LF-Engineering/da-ds/build.GitCommit=$(COMMIT)" cmd/dads/dads.go

fmt: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	./scripts/for_each_go_file.sh "${GO_FMT}"

lint: ## Lint the files
	golint -set_exit_status $(PKG_LIST)

vet: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	go vet $(PKG_LIST)

imports: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	./scripts/for_each_go_file.sh "${GO_IMPORTS}"

usedexports: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	${GO_USEDEXPORTS} ./...

errcheck: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	${GO_ERRCHECK} ./...

test:
	go test -v $(PKG_LIST)

test-coverage:
	./scripts/coverage.sh

check: fmt lint imports vet usedexports errcheck

install: check ${BINARIES}
	${GO_ENV} ${GO_INSTALL} ${GO_BIN_CMDS}

strip: ${BINARIES}
	${STRIP} ${BINARIES}

clean:
	rm -f ${BINARIES}

.PHONY: test build
