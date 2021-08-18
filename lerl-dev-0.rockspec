rockspec_format = "3.0"
package = "lerl"
version = "dev-0"
source = {
   url = "git+https://github.com/Mehgugs/lerl.git"
}
description = {
   homepage = "https://github.com/Mehgugs/lerl",
   license = "MIT"
}

external_dependencies ={
   ZLIB = {
      header = "zlib.h"
   }
}

build = {
   type = "builtin",
   modules = {
      ['lerl'] = {
         sources = {'src/lerl.c'},
         libraries = { "z" },
         incdirs = {'erlpack/cpp', '$(ZLIB_INCDIR)'}
      }
   },
   platforms = {
      windows = {
         modules = {
            lerl = {
               sources = {'src/lerl.c'},
               incdirs = {'erlpack/cpp', '$(ZLIB_INCDIR)'},
               libraries = {
                  "$(ZLIB_LIBDIR)/zlib"
               }
            }
         }
      }
   }
}
