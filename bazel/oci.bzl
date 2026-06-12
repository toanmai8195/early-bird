"""Helpers for packaging services into OCI images with rules_oci."""

load("@rules_go//go:def.bzl", "go_binary")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_load")
load("@rules_pkg//:pkg.bzl", "pkg_tar")

def java_service_image(name, deploy_jar, repo_tag):
    """Wraps a java_binary deploy jar in a distroless/java OCI image.

    Args:
      name: base name; produces :<name>_image and :<name>_load.
      deploy_jar: the <binary>_deploy.jar label (fat jar with all deps).
      repo_tag: image tag for `bazel run :<name>_load`.
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

def go_service_image(name, embed, repo_tag, entrypoint_name = None):
    """Builds a linux/amd64 Go binary and wraps it in a distroless OCI image.

    Args:
      name: base name; produces :<name> (binary), :<name>_image, :<name>_load.
      embed: list of go_library targets to embed (the service's cmd package).
      repo_tag: image tag for `bazel run :<name>_load`, e.g. "early-bird/server:latest".
      entrypoint_name: binary file name inside the image (defaults to name).
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
