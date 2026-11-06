default:
	echo "make test | make test-integration | make publish ver=vX.X.X"

test:
	go test -race -count=1 ./...

# Needs elixir on the PATH. PHOENIX_VERSION="~> 1.7.0" tests against another Phoenix version.
test-integration:
	go test -tags integration -race -count=1 ./...

publish:
ifndef ver
	$(error must give ver=vX.X.X)
endif
	go mod tidy
	git tag $(ver)
	git push origin
	git push origin --tags
	GOPROXY=proxy.golang.org go list -m github.com/TimeRideVR/phx@$(ver)
	http -b https://proxy.golang.org/github.com/TimeRideVR/phx/@v/$(ver).info
	echo "Go add changelog to Releases: https://github.com/TimeRideVR/phx/releases"
