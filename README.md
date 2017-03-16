# Geolocation Jedi: Combining RIPE Atlas and PeeringDB to Geo-locate IP interfaces of Border Routers

This project infers the city-level location of border router IP interfaces 
- interfaces that interconnect the edge routers between two different Autonomous Systems (ASes) -
based on RTT measurements obtained from [RIPE Atlas](https://atlas.ripe.net) probes. 
The [great number and wide coverage](https://atlas.ripe.net/results/maps/network-coverage/) of Atlas probes
enables very accurate latency-based geolocation thanks to the ability to find vantage points in most major cities around the world.
However, the [default limitations](https://atlas.ripe.net/docs/udm/#rate-limits) in probing rates means that it's use every Atlas 
probe to ping each IP interface we want to geolocate, especially if we want to geolocate a large number of interfaces.
To contstrain the problem space and reduce (significanlty) the number of required probes we make the following observation:
the possible locations where a border router can be located are the colocation facilities and datacenters where an AS has presence, 
because these are predominantly the places where inter-domain connectivity is established.


## Motivation

There are multiple methods of IP geolocation, each with different advantages and problems. 
Geolocation databases, such as [Maxmind](https://www.maxmind.com/en/home), are easy to use and accurate for edge hosts, 
but can be very inaccurate for router-level geolocation [1, 2].
Often operators encode geolocation information in the reverse DNS record of an IP address, 
which can provide higher geolocation accuracy [3], 
but DNS-based geolocation requires knowledge of the naming conventions, 
DNS names may not be updated when interfaces change location [4], 
and it's not applicable for IP addresses that do not have reverse DNS records or for reverse DNS records without geolocation hints.

RTT based geolocation informed through PeeringDB facilities and IXP presence data

## References

[1] Poese, Ingmar, et al. "IP geolocation databases: Unreliable?." ACM SIGCOMM Computer Communication Review 41.2 (2011): 53-56.
[2] Shavitt, Yuval, and Noa Zilberman. "A geolocation databases study." IEEE Journal on Selected Areas in Communications 29.10 (2011): 2044-2056.
[3] N. Spring, R. Mahajan, and D. Wetherall. Measuring ISP topologies with Rocketfuel. ACM SIGCOMM conference,
2002.
[4] M. Zhang, Y. Ruan, V. Pai, and J. Rexford. How DNS misnaming distorts Internet topology mapping. In Proc. of USENIX Annual Technical Conference, 2006.