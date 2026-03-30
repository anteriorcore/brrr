{ config, pkgs, ... }:
{
  services.redis.servers.main = {
    enable = true;
    port = 6379;
    openFirewall = true;
    bind = null;
    logLevel = "debug";
    settings.protected-mode = "no";
  };
  services.dynamodb = {
    enable = true;
    openFirewall = true;
  };
  systemd.services.dynamodb = {
    postStart = "wait-for-port ${toString config.services.dynamodb.port}";
    path = [ pkgs.wait-for-port ];
  };
  systemd.services.redis-main = {
    postStart = "wait-for-port ${toString config.services.redis.servers.main.port}";
    path = [ pkgs.wait-for-port ];
  };
}
