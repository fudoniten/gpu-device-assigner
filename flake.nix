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
        helpers = nix-helpers.legacyPackages."${system}";
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
            # The cluster's local registry accepts unauthenticated pushes;
            # without this, deployContainers assumes any non-localhost
            # registry needs credentials and stops to ask for them.
            requireAuth = false;
            entrypoint = let
              gpuDeviceAssigner = self.packages."${system}".gpuDeviceAssigner;
            in [ "${gpuDeviceAssigner}/bin/gpu-device-assigner" ];
            verbose = true;
          };
        };

        checks = {
          # Runs `clojure -M:test` inside the sandbox against the deps pinned
          # in deps-lock.json (test.check included), so no network or $HOME
          # is needed. A plain runCommand fails: the Clojure CLI tries to
          # create ~/.clojure under the sandbox's unwritable /homeless-shelter.
          clojureTests = helpers.mkClojureTests {
            name = "org.fudo/gpu-device-assigner";
            src = ./.;
          };
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
