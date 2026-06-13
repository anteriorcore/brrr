{
  diffutils,
  gnused,
  jq,
  lib,
  package-lock2nix,
  runCommand,
}:

let
  final = package-lock2nix.mkNpmModule {
    src = ./.;
    doInstallCheck = true;
    nativeBuildInputs = [
      diffutils
      jq
    ];
    installCheckPhase =
      let
        fixtures = {
          CmdCheck = "docsync-check checks if two directories' doc tags are in sync.";
          CmdGet = "docsync-get extracts all docsync nodes under a path.";
        };
        fixture = builtins.toFile "fixture" (builtins.toJSON fixtures);
      in
      ''
        f="$(mktemp)"
        $out/bin/docsync-get ${./src} > $f
        diff -u <(jq --sort-keys . ${fixture}) <(jq --sort-keys . $f)
        $out/bin/docsync-get ${./src} CmdGet > $f
        diff -u ${builtins.toFile "test" (fixtures.CmdGet + "\n")} $f
        ! $out/bin/docsync-get ${./src} NonExistentKey
      '';
    passthru.tests.docsync = runCommand "docsync" { nativeBuildInputs = [ final ]; } ''
      docsync-check ${../python/src} ${../typescript/src}
      touch $out
    '';
  };
in
final
