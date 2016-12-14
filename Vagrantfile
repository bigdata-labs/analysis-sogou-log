# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.

Vagrant.configure(2) do |config|


  ANSIBLE_RAW_SSH_ARGS = []
  VAGRANT_VM_PROVIDER = "virtualbox"
  machine_box = "boxcutter/ubuntu1604"

  config.vm.define "nodelab1" do |machine|
    machine.vm.box = machine_box
    machine.vm.hostname = "nodelab1"
    machine.vm.network "private_network", ip: "192.168.7.151"
    machine.vm.provider "virtualbox" do |node|
        node.name = "nodelab1"
        node.memory = 3048
        node.cpus = 2
    end
   end


   config.vm.define "nodelab2" do |machine|
     machine.vm.box = machine_box
     machine.vm.hostname = "nodelab2"
     machine.vm.network "private_network", ip: "192.168.7.152"
     machine.vm.provider "virtualbox" do |node|
         node.name = "nodelab2"
         node.memory = 3048
         node.cpus = 2
     end
    end


    config.vm.define "nodelab3" do |machine|
      machine.vm.box = machine_box
      machine.vm.hostname = "nodelab3"
      machine.vm.network "private_network", ip: "192.168.7.153"
      machine.vm.provider "virtualbox" do |node|
          node.name = "nodelab3"
          node.memory = 3048
          node.cpus = 2
      end
     end

     config.vm.define "nodelab4" do |machine|
       machine.vm.box = machine_box
       machine.vm.hostname = "nodelab4"
       machine.vm.network "private_network", ip: "192.168.7.154"
       machine.vm.provider "virtualbox" do |node|
           node.name = "nodelab4"
           node.memory = 3048
           node.cpus = 2
       end
      end




end
