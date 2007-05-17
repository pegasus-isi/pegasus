appinfo.o: appinfo.cc getif.hh useinfo.hh xml.hh null.hh jobinfo.hh \
  shared.hh time.hh statinfo.hh appinfo.hh maptypes.hh stagejob.hh \
  uname.hh
getif.o: getif.cc getif.hh
jobinfo.o: jobinfo.cc getif.hh jobinfo.hh shared.hh null.hh time.hh \
  xml.hh statinfo.hh useinfo.hh appinfo.hh maptypes.hh stagejob.hh \
  uname.hh mysignal.hh
justparse.o: justparse.cc
k.2.o: k.2.cc null.hh appinfo.hh maptypes.hh time.hh xml.hh statinfo.hh \
  jobinfo.hh shared.hh useinfo.hh stagejob.hh uname.hh
mysignal.o: mysignal.cc mysignal.hh
null.o: null.cc null.hh
quote.o: quote.cc quote.hh shared.hh
stagejob.o: stagejob.cc statinfo.hh null.hh xml.hh stagejob.hh \
  maptypes.hh jobinfo.hh shared.hh time.hh useinfo.hh appinfo.hh uname.hh
statinfo.o: statinfo.cc statinfo.hh null.hh xml.hh time.hh
time.o: time.cc time.hh
uname.o: uname.cc uname.hh xml.hh
useinfo.o: useinfo.cc useinfo.hh xml.hh null.hh time.hh
xml.o: xml.cc xml.hh
