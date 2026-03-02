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
          brrrScope = pkgs.callPackage ../brrr-scope.nix { inherit inputs; };
        in
        {
          brrr.scope = brrrScope;

          packages = rec {
            inherit (brrrScope)
              # keep-sorted start
              brrr
              brrr-demo-py
              brrr-demo-ts
              brrr-venv
              brrr-venv-test
              brrrts
              docsync
              npm-version-to-git
              uv
              # keep-sorted end
              ;

            default = brrr-venv;
          };

          checks =
            # TODO: make docsync standalone flakemodule
            brrrScope.docsync.tests;
        };
    }
  );
}
