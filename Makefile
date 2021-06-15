.PHONY: default build setup deps test test-all lint coverage watch dist

default: build

setup: setup-tools build

setup-tools: -setup-poetry -setup-direnv -setup-keyfiles -setup-docker

-setup-poetry:
	@if ! command -v poetry &> /dev/null; then ${MAKE} install-poetry; fi

install-poetry:
	curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | python -

-setup-direnv:
	@if ! command -v direnv &> /dev/null; then ${MAKE} install-direnv; fi
	direnv allow .

install-direnv:
	brew install direnv -q

-setup-docker: -maybe-install-docker -pull-images -maybe-configure-git-hook

-maybe-install-docker:
	# @if ! command -v docker-compose &> /dev/null; then ${MAKE} install-docker; fi

install-docker:
	# brew install docker-compose -q

-pull-images:
	# docker-compose pull

-setup-keyfiles: -maybe-configure-git-hook

# Disable using the environment variable DEFER_GIT_HOOK=1.  See the "Note: key
# file permissions" in the [README](./README.md#note-key-file-permissions) for
# details on this.
-maybe-configure-git-hook:
	# @if ! [[ "${DEFER_GIT_HOOK}" -eq "1" ]] ; then ${MAKE} configure-git-hook; fi

configure-git-hook:
	# # append the chmod command to the post-checkout hook
	# echo 'chmod 0600 `git rev-parse --show-toplevel`/test/fixtures/*key*' >> .git/hooks/post-checkout
	# # mark the hook as executable
	# chmod a+x .git/hooks/post-checkout
	# # execute it
	# sh .git/hooks/post-checkout

deps: 
	poetry install

build: deps test-all

-docker-start:
	# docker-compose up -d
	# @sleep 1

-docker-stop:
	# docker-compose stop

-docker-restart: -docker-stop -docker-start

test-all: -docker-restart test -docker-stop coverage-report lint

pytest = pytest -q --log-file-level=WARN
poerun = poetry run
direxc = direnv exec . ${poerun}
covage = coverage run -m
covtst = ${covage} ${pytest}

lint:
	${poerun} flakehell lint

coverage: test coverage-report

coverage-report:
	${poerun} coverage html -d test/codecoverage
	${poerun} coverage report -m --skip-empty --skip-covered --fail-under=70


test-units:
	${poerun} ${pytest} -m "not integration"

test-integrations:
	${direxc} ${pytest} -m "integration"

test:
	${direxc} ${covtst}

ci: 
	${poerun} ${covtst}

watch:
	find src test -type f -name '*.py' | entr make test

dist: build -dist-package -upload-package

-dist-package:
	poetry build

# note that this does NOT use poetry's publish mechanism, which seems
# problematic (immature, maybe?) when using private repositories. a task
# for another day, perhaps
-upload-package:
	${poerun} twine upload dist/* --repository-url https://push.fury.io/bonobos-eng -p ""
