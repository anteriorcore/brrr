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
              brrrts
              brrr-venv
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
            brrrScope.docsync.tests // import ../brrr-demo.test.nix { inherit self pkgs; };
        };
    }
  );
}
