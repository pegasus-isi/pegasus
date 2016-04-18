Vagrant.configure('2') do |config|

  config.vm.box = 'geerlingguy/centos7'

  config.vm.define 'pegasus-dev', primary: true do |dev|
    dev.vm.hostname = 'pegasus-dev'
    #dev.vm.network :forwarded_port, :guest => 5000, :host => 5000
  end

  config.vm.provider "virtualbox" do |v|
    v.memory = 1024
    v.customize ["guestproperty", "set", :id, "/VirtualBox/GuestAdd/VBoxService/--timesync-set-threshold", 10000]
  end

  config.vm.provision :shell, :path => 'Vagrantsetup.sh'
end
