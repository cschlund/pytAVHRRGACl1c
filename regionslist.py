
#
# subroutines for plotting AVHRR GAC L1c data
# C. Schlundt, June 2014
#
# -------------------------------------------------------------------
#dictionaries, please include new regions alphabetically!
# gmt     => '-R-180/180/-90/90'

REGIONS = dict(

afr = { "nam" : "Africa", 
	"par" : [-90, 91, 5],
	"mer" : [-180, 181, 10],
	"geo" : {"lon_0" : 20, "lat_0" : 0, "height" : 9000e3, "width" : 8500e3,
		 "projection" : 'stere', "resolution" : 'i'} },
		 
eur = { "nam" : "Europe", 
	"par" : [-90, 91, 5],
	"mer" : [-180, 181, 10],
	"geo" : {"lon_0" : 10, "lat_0" : 52, "height" : 4000e3, "width" : 4000e3,
		 "projection" : 'stere', "resolution" : 'i'} },

ger = { "nam" : "Germany", 
	"par" : [-90, 91, 2],
	"mer" : [-180, 181, 2],
	"geo" : {"lon_0" : 10, "lat_0" : 51, "height" : 1000e3, "width" : 1000e3,
		 "projection" : 'stere', "resolution" : 'i'} },

glo = { "nam" : "Global", 
	"par" : [-90, 91, 30],
	"mer" : [-180, 181, 60],
	"geo" : {"llcrnrlat" : -90, "urcrnrlat" : 90, "llcrnrlon" : -180, "urcrnrlon" : 180, 
		 "projection" : 'cyl', "resolution" : 'c'} },

rus = { "nam" : "Russia", 
	"par" : [-90, 91, 5],
	"mer" : [-180, 181, 60],
	"geo" : {"lon_0" : 100, "lat_0" : 70, "height" : 5000e3, "width" : 9000e3,
		 "projection" : 'stere', "resolution" : 'i'} }
)