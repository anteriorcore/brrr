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
        $out/bin/docsync-get ${./src} > full-out
        diff -u <(jq --sort-keys . ${fixture}) <(jq --sort-keys . full-out)

        $out/bin/docsync-get ${./src} CmdGet > single-out
        diff -u ${builtins.toFile "test" (fixtures.CmdGet + "\n")} single-out

        ! $out/bin/docsync-get ${./src} KeyWhichDoesntExistForTestingSake
        ! $out/bin/docsync-get ${./src} CmdGet extra-arg
      '';
    passthru.tests.docsync = runCommand "docsync" { nativeBuildInputs = [ final ]; } ''
      docsync-check ${../python/src} ${../typescript/src}
      touch $out
    '';
  };
in
final
