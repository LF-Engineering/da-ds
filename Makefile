GO_LIB_FILES=affs.go context.go const.go ds.go dsconfluence.go dsgerrit.go dsgit.go dsgroupsio.go dsjira.go dsstub.go email.go es.go error.go exec.go json.go log.go mbox.go redacted.go sql.go threads.go time.go utils.go uuid.go
GO_BIN_FILES=cmd/dads/dads.go
GO_TEST_FILES=context_test.go email_test.go regexp_test.go time_test.go threads_test.go uuid_test.go
GO_LIBTEST_FILES=test/time.go
GO_BIN_CMDS=github.com/LF-Engineering/da-ds/cmd/dads
#for race CGO_ENABLED=1
#GO_ENV=CGO_ENABLED=1
GO_ENV=CGO_ENABLED=0
GO_BUILD=go build -ldflags '-s -w'
#GO_BUILD=go build -ldflags '-s -w' -race
GO_INSTALL=go install -ldflags '-s'
GO_FMT=gofmt -s -w
GO_LINT=golint -set_exit_status
GO_VET=go vet
GO_CONST=goconst
GO_IMPORTS=goimports -w
GO_USEDEXPORTS=usedexports
GO_ERRCHECK=errcheck -asserts -ignore '[FS]?[Pp]rint*' -ignoretests
GO_TEST=go test
BINARIES=dads
STRIP=strip

all: check ${BINARIES}

dads: cmd/dads/dads.go ${GO_LIB_FILES}
	 ${GO_ENV} ${GO_BUILD} -o dads cmd/dads/dads.go

fmt: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	./for_each_go_file.sh "${GO_FMT}"

lint: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	./for_each_go_file.sh "${GO_LINT}"

vet: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	./vet_files.sh "${GO_VET}"

imports: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	./for_each_go_file.sh "${GO_IMPORTS}"

const: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	${GO_CONST} ./...

usedexports: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	${GO_USEDEXPORTS} ./...

errcheck: ${GO_BIN_FILES} ${GO_LIB_FILES} ${GO_TEST_FILES} ${GO_LIBTEST_FILES}
	${GO_ERRCHECK} ./...

test:
	${GO_TEST} ${GO_TEST_FILES}

check: fmt lint imports vet const usedexports errcheck

install: check ${BINARIES}
	${GO_ENV} ${GO_INSTALL} ${GO_BIN_CMDS}

strip: ${BINARIES}
	${STRIP} ${BINARIES}

clean:
	rm -f ${BINARIES}

.PHONY: test
