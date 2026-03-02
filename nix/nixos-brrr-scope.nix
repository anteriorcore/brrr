{
  pkgs,
  lib,
  inputs,
  ...
}:
{
  options.brrr.scope = lib.mkOption { type = lib.types.raw; };
  config.brrr.scope = pkgs.callPackage ./brrr-scope.nix { inherit inputs; };
}
