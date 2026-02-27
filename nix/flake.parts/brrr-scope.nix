{
  inputs,
  self,
  flake-parts-lib,
  ...
}:
{
  options.perSystem = flake-parts-lib.mkPerSystemOption (
    { pkgs, lib, ... }:
    {
      options.brrr.scope = lib.mkOption { type = lib.types.raw; };
      config =
        let
          brrrScope = lib.makeScope pkgs.newScope (
            scopeSelf:
            let
              inherit (scopeSelf) callPackage;
            in
            rec {
              inherit (pkgs) process-compose redis uv;
              inherit (inputs) pyproject-build-systems pyproject-nix uv2nix;
              inherit (brrrpy) brrr brrr-venv brrr-venv-test;
              inherit (brrrts) npm-version-to-git;

              nodejs = pkgs.nodejs_24;
              python = pkgs.python313;

              # TODO: move all these to ./packages
              brrrts = callPackage ../../typescript/package.nix { };
              brrrpy = callPackage ../../python/package.nix { };
              docsync = callPackage ../../docsync/package.nix { };

              package-lock2nix = pkgs.callPackage inputs.package-lock2nix.lib.package-lock2nix {
                inherit nodejs;
              };

              brrr-demo-py = pkgs.stdenvNoCC.mkDerivation {
                name = "brrr-demo.py";
                dontUnpack = true;
                installPhase = ''
                  mkdir -p $out/bin
                  cp ${../../brrr_demo.py} $out/bin/brrr_demo.py
                '';
                buildInputs = [ brrrpy.brrr-venv-test ];
                # The patch phase will automatically use the python from the venv as
                # the interpreter for the demo script.
                meta.mainProgram = "brrr_demo.py";
              };

              brrr-demo-ts = brrrts.overrideAttrs { meta.mainProgram = "brrr-demo"; };
            }
          );
        in
        {
          brrr.scope = brrrScope;

          packages = rec {
            inherit (brrrScope)
              brrr-venv
              brrr
              brrrts
              docsync
              brrr-demo-py
              brrr-demo-ts
              brrr-venv-test
              npm-version-to-git
              ;

            default = brrr-venv;
          };

          checks =
            # TODO: make these into indivisual flake modules
            brrrScope.docsync.tests
            // import ../brrr-integration.test.nix { inherit self pkgs; }
            // import ../brrr-demo.test.nix { inherit self pkgs; };
        };
    }
  );
}
