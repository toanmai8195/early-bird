"""Helpers for packaging services into OCI images with rules_oci.

Macros
------
go_image        — Go binary → native local binary + linux/amd64 OCI image
go_service_image — (legacy) thin wrapper kept for backward compatibility
java_service_image — deploy-jar → distroless/java OCI image
"""

load("@rules_go//go:def.bzl", "go_binary", "go_library")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load("@rules_pkg//:pkg.bzl", "pkg_tar")

def go_image(
        name,
        srcs,
        importpath,
        deps = [],
        exposed_ports = [],
        repo_tag = None,
        visibility = ["//visibility:public"],
        data = [],
        args = []):
    """Builds a Go binary, packages it into a distroless OCI image, and exposes
    a local (native-arch) run target alongside a docker-loadable image.

    Produced targets:
      :{name}        — native binary for `bazel run` on the dev machine.
      :{name}_image  — OCI image (linux/amd64, static distroless).
      :{name}_load   — `bazel run :{name}_load` to load the image into Docker.

    Args:
      name:          Base name for all generated targets.
      srcs:          Go source files.
      importpath:    Go import path for the library (e.g. "github.com/tm/loadtest/pg").
      deps:          Go library dependencies.
      exposed_ports: Ports to expose in the container (strings, e.g. ["8080"]).
      repo_tag:      Docker image tag. Defaults to "early-bird/{name}:latest".
      visibility:    Bazel visibility for all targets.
      data:          Data files available to the local binary at runtime.
      args:          Default CLI arguments for the local binary.
    """

    if repo_tag == None:
        repo_tag = "early-bird/%s:latest" % name

    lib_name = name + "_lib"
    linux_bin_name = name + "_linux"

    # Library shared by both the local and the docker binary.
    go_library(
        name = lib_name,
        srcs = srcs,
        importpath = importpath,
        deps = deps,
        visibility = ["//visibility:private"],
    )

    # Local binary: native arch so `bazel run :{name}` works on macOS / Linux dev boxes.
    go_binary(
        name = name,
        embed = [":" + lib_name],
        data = data,
        args = args,
        visibility = visibility,
    )

    # Docker binary: static linux/amd64 for the container layer.
    go_binary(
        name = linux_bin_name,
        embed = [":" + lib_name],
        goos = "linux",
        goarch = "amd64",
        pure = "on",
        visibility = ["//visibility:private"],
    )

    # Tar layer: places the linux binary at /usr/local/bin/.
    # The binary is named {name}_linux inside the image (pkg_tar preserves the
    # go_binary output name); the entrypoint below references that exact name.
    pkg_tar(
        name = name + "_layer",
        srcs = [":" + linux_bin_name],
        package_dir = "/usr/local/bin",
        visibility = ["//visibility:private"],
    )

    oci_image(
        name = name + "_image",
        base = "@distroless_static",
        tars = [":" + name + "_layer"],
        entrypoint = ["/usr/local/bin/" + linux_bin_name],
        exposed_ports = exposed_ports,
        visibility = visibility,
    )

    oci_load(
        name = name + "_load",
        image = ":" + name + "_image",
        repo_tags = [repo_tag],
        visibility = visibility,
    )

def go_service_image(name, embed, repo_tag, entrypoint_name = None):
    """(Legacy) Builds a linux/amd64 Go binary and wraps it in a distroless OCI image.

    Prefer go_image for new targets — it also produces a usable local binary.

    Args:
      name:             Base name; produces :<name> (binary), :<name>_image, :<name>_load.
      embed:            go_library targets to embed (the service's cmd package).
      repo_tag:         Image tag for `bazel run :<name>_load`.
      entrypoint_name:  Binary file name inside the image (defaults to name).
    """
    bin_name = entrypoint_name or name

    go_binary(
        name = name,
        embed = embed,
        goos = "linux",
        goarch = "amd64",
        pure = "on",
        visibility = ["//visibility:public"],
    )

    pkg_tar(
        name = name + "_layer",
        srcs = [":" + name],
        package_dir = "/usr/local/bin",
    )

    oci_image(
        name = name + "_image",
        base = "@distroless_static",
        tars = [":" + name + "_layer"],
        entrypoint = ["/usr/local/bin/" + bin_name],
        visibility = ["//visibility:public"],
    )

    oci_load(
        name = name + "_load",
        image = ":" + name + "_image",
        repo_tags = [repo_tag],
        visibility = ["//visibility:public"],
    )

def java_service_image(name, deploy_jar, repo_tag):
    """Wraps a java_binary deploy jar in a distroless/java OCI image.

    Args:
      name:        Base name; produces :<name>_image and :<name>_load.
      deploy_jar:  The <binary>_deploy.jar label (fat jar with all deps).
      repo_tag:    Image tag for `bazel run :<name>_load`.
    """
    pkg_tar(
        name = name + "_layer",
        srcs = [deploy_jar],
        package_dir = "/app",
    )

    oci_image(
        name = name + "_image",
        base = "@distroless_java",
        tars = [":" + name + "_layer"],
        entrypoint = ["java", "-jar", "/app/" + name + "_deploy.jar"],
        visibility = ["//visibility:public"],
    )

    oci_load(
        name = name + "_load",
        image = ":" + name + "_image",
        repo_tags = [repo_tag],
        visibility = ["//visibility:public"],
    )
