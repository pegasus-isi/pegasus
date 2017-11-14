#!/bin/bash

set -e


#-----------------------
# Basic Desktop Packages
#-----------------------

yum -y install bzip2 epel-release @x11

yum -y groups install "Xfce"


#----------------------------
# Install & Configure Firefox
#----------------------------

curl --location --output firefox.tar.bz2 \
     "https://download.mozilla.org/?product=firefox-latest&os=linux64&lang=en-US"

tar jxvf firefox.tar.bz2 --directory /usr/share

rm --force firefox.tar.bz2

cat > /usr/share/firefox/defaults/pref/local-settings.js <<EOT
pref("general.config.filename", "mozilla.cfg");
pref("general.config.obscure_value", 0);
EOT


cat > /usr/share/firefox/mozilla.cfg <<EOT
defaultPref("general.warnOnAboutConfig", false);
defaultPref("startup.homepage_welcome_url", "");
defaultPref("startup.homepage_welcome_url.additional", "");
defaultPref("startup.homepage_override_url", "");

pref("browser.shell.checkDefaultBrowser", false);
pref("browser.tabs.warnOnClose", false);
pref("browser.startup.homepage", "https://localhost:5000/|https://pegasus.isi.edu/documentation|https://pegasus.isi.edu");

lockPref("general.warnOnAboutConfig", false);
EOT


#--------------------------------
# Create Terminal Icon on Desktop
#--------------------------------

mkdir --parent /home/${USERNAME}/Desktop

# Desktop: Add shortcut for Terminal
cat > /home/${USERNAME}/Desktop/Terminal.desktop <<EOT
[Desktop Entry]
Version=1.0
Type=Application
Name=Terminal
Comment=Terminal
Exec=/usr/bin/xfce4-terminal
Icon=utilities-terminal
Path=
Terminal=false
StartupNotify=false
EOT

# Desktop: Add shortcut for Firefox
cat > /home/${USERNAME}/Desktop/Firefox.desktop <<EOT
[Desktop Entry]
Version=1.0
Type=Application
Name=Firefox
Comment=Firefox
Exec=/usr/share/firefox/firefox
Icon=/usr/share/firefox/browser/icons/mozicon128.png
Path=
Terminal=false
StartupNotify=false
EOT

chown ${USERNAME}:${USERNAME} /home/${USERNAME}/Desktop/{Terminal,Firefox}.desktop
chmod +x                      /home/${USERNAME}/Desktop/{Terminal,Firefox}.desktop


#-------------------------
# Start Desktop on Startup
#-------------------------

systemctl set-default graphical.target
