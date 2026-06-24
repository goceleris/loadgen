package loadgen

import (
	"runtime/debug"
	"strings"
)

// modulePath must match go.mod; used to locate loadgen in a consumer's
// dependency list when resolving Version from build info.
const modulePath = "github.com/goceleris/loadgen"

// fallbackVersion is reported when build info carries no usable module
// version for loadgen ((devel) builds, tests, directory replaces). Keep in
// sync with the release tag.
const fallbackVersion = "1.4.11"

// Version is the loadgen release version, stamped into Result.LoadgenVersion
// so downstream consumers (probatorium, perfmatrix) can record which loadgen
// build produced a given run. Resolved from runtime/debug build info at
// startup — correct both when loadgen is the main module of a tagged build
// and when it is a dependency — with fallbackVersion as the last resort.
var Version = versionFromBuildInfo(debug.ReadBuildInfo())

func versionFromBuildInfo(bi *debug.BuildInfo, ok bool) string {
	if !ok || bi == nil {
		return fallbackVersion
	}
	if bi.Main.Path == modulePath {
		if v := cleanModuleVersion(bi.Main.Version); v != "" {
			return v
		}
		return fallbackVersion
	}
	for _, dep := range bi.Deps {
		if dep.Path != modulePath {
			continue
		}
		ver := dep.Version
		if dep.Replace != nil {
			ver = dep.Replace.Version
		}
		if v := cleanModuleVersion(ver); v != "" {
			return v
		}
		break
	}
	return fallbackVersion
}

// cleanModuleVersion normalises a module version ("v1.4.7") to the bare
// form loadgen has always reported ("1.4.7"). Returns "" when the version
// carries no release information.
func cleanModuleVersion(v string) string {
	if v == "" || v == "(devel)" {
		return ""
	}
	return strings.TrimPrefix(v, "v")
}
