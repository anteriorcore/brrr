{
  inputs,
  lib,
  pkgs,
  ...
}:
lib.makeScope pkgs.newScope (
  scopeSelf:
  let
    inherit (scopeSelf) callPackage;

    buildImageForPackage = (
      name:
      pkgs.dockerTools.buildLayeredImage {
        inherit name;
        tag = "latest";
        config.Entrypoint = [ (lib.getExe scopeSelf.${name}) ];
      }
    );
  in
  rec {
    inherit (pkgs) process-compose redis uv;
    inherit (inputs) pyproject-build-systems pyproject-nix uv2nix;
    inherit (brrrpy) brrr brrr-venv brrr-venv-test;
    inherit (brrrts) npm-version-to-git;

    nodejs = pkgs.nodejs_24;
    python = pkgs.python313;

    # TODO: move all these to ./packages
    brrrts = callPackage ../typescript/package.nix { };
    brrrpy = callPackage ../python/package.nix { };
    docsync = callPackage ../docsync/package.nix { };

    package-lock2nix = callPackage inputs.package-lock2nix.lib.package-lock2nix {
      inherit nodejs;
      # NOMERGE infinite recursion
      overrideScope = prev: final: { };
    };

    brrr-demo-py = pkgs.stdenvNoCC.mkDerivation {
      name = "brrr-demo.py";
      dontUnpack = true;
      installPhase = ''
        mkdir -p $out/bin
        cp ${../brrr_demo.py} $out/bin/brrr_demo.py
      '';
      buildInputs = [ brrrpy.brrr-venv-test ];
      # The patch phase will automatically use the python from the venv as
      # the interpreter for the demo script.
      meta.mainProgram = "brrr_demo.py";
    };

    brrr-demo-ts = brrrts.overrideAttrs { meta.mainProgram = "brrr-demo"; };

    # NOMERGE only build in linux?
    docker-py = buildImageForPackage "brrr-demo-py";
    docker-ts = buildImageForPackage "brrr-demo-ts";
  }
)
