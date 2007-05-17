#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>

#if defined(__CYGWIN__) || defined(_windows)
#define PATHSEP_CHAR ';'
#define FILESEP_CHAR '\\'
#define PATHSEP_STRING ";"
#define FILESEP_STRING "\\"
#else
#define PATHSEP_CHAR ':'
#define FILESEP_CHAR '/'
#define PATHSEP_STRING ":"
#define FILESEP_STRING "/"
#endif

#include <iostream>
#include <string>
#include <vector>
#include <set>

typedef std::string<char>   String;
typedef std::vector<String> StringVector;
typedef std::set<String>    StringSet;

inline
String
catfile( String dir, String file )
{ 
  return ( dir + FILESEP_STRING + file ); 
}

void
getChimeraJars( String vdlhome, 
		StringVector& chimera, 
		StringSet& baselist, const StringSet avoid )
{
  String libdir = catfile( vdlhome, "lib" );
  DIR* lib = opendir( libdir.c_str() );
  if ( lib == NULL ) {
    std::cerr <<  "unable to opendir " << libdir.c_str() << ": " << errno
	      << ": " << strerror(errno) << std::endl;
    exit(1);
  }

  struct stat st;
  struct dirent* d;
  while ( (d = readdir(lib)) ) {
    String basename( d->d_name );
    String file = catfile( libdir, basename );

    if ( stat( file.c_str(), &st ) == 0 && S_ISREG(st.st_mode) && 
	 (st.st_mode & S_IXUSR) == S_IXUSR ) {
      // candidate
      if ( basename.rfind(".jar") != String::npos || basename.rfind(".zip") != String::npos ) {
	if ( avoid.find(basename) == avoid.end() && avoid.find(file) == avoid.end() ) {
	  chimera.push_back( file );
	  baselist.insert( basename );
	}
      }
    }
  }

  closedir(lib);
}

int
main( int argc, char* argv[] )
{
  StringVector chimeraFull, classPathFull;
  StringSet    chimeraBase, duplicates, avoid;

  for ( int i=1; i<argc; ++i ) avoid.insert( argv[i] );
  
  char* vdlhome = getenv("PEGASUS_HOME");
  if ( vdlhome == 0 ) {
    std::cerr << "unknown environment variable: PEGASUS_HOME" << std::endl;
    return 1;
  } else {
    getChimeraJars( vdlhome, chimeraFull, chimeraBase, avoid );
  }

  char* classpath = getenv("CLASSPATH");
  if ( classpath && *classpath ) {
    // only if we know a CLASSPATH w/ some content
    String cp(classpath);
    struct stat st;
    // split at PATHSEP_CHAR
    for ( String::size_type start, offset = start = 0; 
	  offset != String::npos && start < cp.size(); 
	  start = offset+1 ) {
      while ( offset < cp.size() && cp[offset] == PATHSEP_CHAR ) ++offset;
      if ( offset >= cp.size() ) break;
      while ( offset < cp.size() && cp[offset] != PATHSEP_CHAR ) ++offset;
      if ( offset >= cp.size() ) offset = String::npos;

      // extract substring and its basename
      String basename;
      String component = String( cp, start, offset-start );
      String::size_type pos = component.rfind(FILESEP_CHAR);
      if ( pos == String::npos || pos == component.size() )
	basename = component;
      else
	basename = String( component, pos+1 );

      // if component exists in filesystem and has a length
      if ( stat( component.c_str(), &st ) == 0 && component.length() > 1 ) {
	// if component is either a directory OR not a Chimera library
	if ( S_ISDIR(st.st_mode) || chimeraBase.find(basename) == chimeraBase.end() ) {
	  // add to list, if not already in or ok
	  if ( duplicates.find(component) == duplicates.end() &&
	       avoid.find(basename) == avoid.end() &&
	       avoid.find(component) == avoid.end() ) {
	    duplicates.insert(component);
	    classPathFull.push_back(component);
	  }
	}
      }
    }

    // print all CLASSPATH components that we want to keep
    for ( StringVector::const_iterator i = classPathFull.begin();
	  i != classPathFull.end(); i++ ) {
      if ( i != classPathFull.begin() ) std::cout << PATHSEP_STRING;
      std::cout << i->c_str();
    }
    if ( ! classPathFull.empty() ) std::cout << PATHSEP_STRING;
  }

  // print Chimera files found
  for ( StringVector::const_iterator i=chimeraFull.begin();
	i != chimeraFull.end(); i++ ) {
    if ( i != chimeraFull.begin() ) std::cout << PATHSEP_STRING;
    std::cout << i->c_str();
  }
  std::cout << std::endl;

  return 0;
}
