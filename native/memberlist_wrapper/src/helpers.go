package main

import "C"
import "net"

func setErr(errOut **C.char, msg string) {
	if errOut != nil {
		*errOut = C.CString(msg)
	}
}

func localIPs(addr string) ([]net.IP, error) {
	ip := net.ParseIP(addr)
	if ip != nil && !ip.IsUnspecified() {
		return []net.IP{ip}, nil
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	var ips []net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			var candidate net.IP
			switch v := a.(type) {
			case *net.IPNet:
				candidate = v.IP
			case *net.IPAddr:
				candidate = v.IP
			}
			if candidate != nil && candidate.To4() != nil {
				ips = append(ips, candidate)
			}
		}
	}
	if len(ips) == 0 {
		return nil, net.ErrWriteToConnected
	}
	return ips, nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	b := make([]byte, 0, 8)
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
