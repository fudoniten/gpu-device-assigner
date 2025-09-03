{
  description = "GPU Device Assigner -- Kubernetes job to assign jobs to GPUs.";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-25.05";
    utils.url = "github:numtide/flake-utils";
    nix-helpers = {
      url = "github:fudoniten/fudo-nix-helpers";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, utils, nix-helpers, ... }:
    utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages."${system}";
        helpers = nix-helpers.packages."${system}";
        cljLibs = { };
      in {
        packages = rec {
          default = gpuDeviceAssigner;

          gpuDeviceAssigner = helpers.mkClojureBin {
            name = "org.fudo/gpu-device-assigner";
            primaryNamespace = "gpu-device-assigner.cli";
            src = ./.;
          };

          deployContainer = helpers.deployContainers {
            name = "gpu-device-assigner";
            repo = "registry.kube.sea.fudo.link";
            tags = [ "latest" ];
            entrypoint = let
              gpuDeviceAssigner = self.packages."${system}".gpuDeviceAssigner;
            in [ "${gpuDeviceAssigner}/bin/gpu-device-assigner" ];
            verbose = true;
          };
        };

        checks = {
          clojureTests = pkgs.runCommand "clojure-tests" { } ''
            mkdir -p $TMPDIR
            cd $TMPDIR
            ${pkgs.clojure}/bin/clojure -M:test
          '';
        };

        devShells = rec {
          default = updateDeps;
          updateDeps = pkgs.mkShell {
            buildInputs = [ (helpers.updateClojureDeps cljLibs) ];
          };
          gpuDeviceAssignerServer = pkgs.mkShell {
            buildInputs = with self.packages."${system}"; [ gpuDeviceAssigner ];
          };
        };

        apps = rec {
          default = deployContainer;
          deployContainer = {
            type = "app";
            program =
              let deployContainer = self.packages."${system}".deployContainer;
              in "${deployContainer}/bin/deployContainers";
          };
        };
      });
}
