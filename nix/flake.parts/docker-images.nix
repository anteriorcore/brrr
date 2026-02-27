{ ... }:
{
  perSystem =
    {
      config,
      pkgs,
      lib,
      ...
    }:
    let
      brrrScope = config.brrr.scope;

      buildImageForPackage = (
        name:
        pkgs.dockerTools.buildLayeredImage {
          inherit name;
          tag = "latest";
          config.Entrypoint = [ (lib.getExe brrrScope.${name}) ];
        }
      );
    in
    {
      packages = lib.mkIf pkgs.stdenv.isLinux {
        docker-py = buildImageForPackage "brrr-demo-py";
        docker-ts = buildImageForPackage "brrr-demo-ts";
      };
    };
}
