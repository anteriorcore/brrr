{
  gnused,
  lib,
  package-lock2nix,
  runCommand,
}:

let
  final = package-lock2nix.mkNpmModule {
    src = ./.;
    passthru.tests.docsync = runCommand "docsync" { nativeBuildInputs = [ final ]; } ''
      docsync-check ${../python} ${../typescript}
      touch $out
    '';
  };
in
final
