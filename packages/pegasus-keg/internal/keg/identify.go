package keg

import (
	"net"
	"os"
	"strings"
)

// Name lookups, replaced by tests.
var (
	lookupAddr = net.LookupAddr
	lookupHost = net.LookupHost
)

// identify returns the "IP addr and hostname" line written at the end of
// every output file and to the log file.
func identify() string {
	return "IP addr and hostname: " + describeHost(primaryIPv4(), localHostname()) + "\n"
}

// describeHost formats ip as "<ip> (VPN)" for private addresses, otherwise
// as "<ip> (<name>)" using a reverse lookup (or a forward lookup of name
// when no usable address was found), falling back to the bare IP.
func describeHost(ip net.IP, name string) string {
	if ip.IsPrivate() {
		return ip.String() + " (VPN)"
	}
	if ip.IsUnspecified() {
		if addrs, err := lookupHost(name); err == nil {
			for _, a := range addrs {
				if v4 := net.ParseIP(a).To4(); v4 != nil {
					return v4.String() + " (" + name + ")"
				}
			}
		}
	} else if names, err := lookupAddr(ip.String()); err == nil && len(names) > 0 {
		return ip.String() + " (" + strings.TrimSuffix(names[0], ".") + ")"
	}
	return ip.String()
}

// localHostname returns $HOSTNAME, or the kernel's node name.
func localHostname() string {
	if h := os.Getenv("HOSTNAME"); h != "" {
		return h
	}
	h, _ := os.Hostname()
	return h
}

// primaryIPv4 guesses the host's primary IPv4 address: the first address of
// an up, non-loopback interface that is neither private (RFC 1918) nor
// link-local, else the first such private/link-local address, else 0.0.0.0.
func primaryIPv4() net.IP {
	ifaces, err := net.Interfaces()
	if err != nil {
		return net.IPv4zero
	}
	var first net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipnet.IP.To4()
			if ip == nil || ip.IsLoopback() {
				continue
			}
			if !ip.IsPrivate() && !ip.IsLinkLocalUnicast() {
				return ip
			}
			if first == nil {
				first = ip
			}
		}
	}
	if first != nil {
		return first
	}
	return net.IPv4zero
}
