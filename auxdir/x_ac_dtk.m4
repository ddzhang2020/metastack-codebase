##*****************************************************************************
#  AUTHOR:
#    Advanced Micro Devices
#
#  SYNOPSIS:
#    X_AC_DTK
#
#  DESCRIPTION:
#    Determine if HYGON's DTK API library exists
##*****************************************************************************

AC_DEFUN([X_AC_DTK],
[

  # /opt/dtk is the current default location.
  # /opt/dtk/rocm_smi was the default location for before to 5.2.0
  # We will use a for loop to check for both.
  # Unless _x_ac_dtk_dirs is overwritten with --with-dtk
  _x_ac_dtk_dirs="/opt/hyhal /opt/hyhal/rocm_smi"

  AC_ARG_WITH(
    [dtk],
    AS_HELP_STRING(--with-dtk=PATH, Specify path to dtk installation),
    [AS_IF([test "x$with_dtk" != xno && test "x$with_dtk" != xyes],
           [_x_ac_dtk_dirs="$with_dtk"])])

  if [test "x$with_dtk" = xno]; then
     AC_MSG_NOTICE([support for dtk disabled])
  else
    AC_MSG_CHECKING([whether DTK/ROCm in installed in this system])
    # Check for DTK header and library in the default location
    # or in the location specified during configure
    #
    # NOTE: Just because this is where we are looking and finding the
    # libraries they must be in the ldcache when running as that is what the
    # card will be using.
    AC_MSG_RESULT([])
    for _x_ac_dtk_dir in $_x_ac_dtk_dirs; do
      cppflags_save="$CPPFLAGS"
      ldflags_save="$LDFLAGS"
      DTK_CPPFLAGS="-I$_x_ac_dtk_dir/include"
      CPPFLAGS="$DTK_CPPFLAGS"
      DTK_LIB_DIR="$_x_ac_dtk_dir/lib"
      LDFLAGS="-L$DTK_LIB_DIR"
      AS_UNSET([ac_cv_header_rocm_smi_h1])
      AS_UNSET([ac_cv_lib_rocm_smi64_rsmi_init1])
      AS_UNSET([ac_cv_lib_rocm_smi64_dev_drm_render_minor_get1])
      AC_CHECK_HEADER([rocm_smi/rocm_smi.h], [ac_dtk_h=yes], [ac_dtk_h=no])
      AC_CHECK_LIB([rocm_smi64], [rsmi_init], [ac_dtk_l=yes], [ac_dtk_l=no])
      AC_CHECK_LIB([rocm_smi64], [rsmi_dev_drm_render_minor_get], [ac_dtk_version=yes], [ac_dtk_version=no])
      CPPFLAGS="$cppflags_save"
      LDFLAGS="$ldflags_save"
      if test "$ac_dtk_l" = "yes" && test "$ac_dtk_h" = "yes"; then
        if test "$ac_dtk_version" = "yes"; then
          ac_dtk="yes"
          AC_DEFINE(HAVE_DTK, 1, [Define to 1 if DTK library found])
	  AC_SUBST(DTK_CPPFLAGS)
          break;
        fi
      fi
    done

    # Only print errors/wanrings if both _x_ac_dtk_dirs don't work
    if test "$ac_dtk_l" = "yes" && test "$ac_dtk_h" = "yes"; then
      if test "$ac_dtk_version" != "yes"; then
        if test -z "$with_dtk"; then
          AC_MSG_WARN([upgrade to newer version of ROCm/rsmi])
        else
          AC_MSG_ERROR([upgrade to newer version of ROCm/rsmi])
        fi
      fi
    else
      if test -z "$with_dtk"; then
        AC_MSG_WARN([unable to locate librocm_smi64.so and/or rocm_smi.h])
      else
        AC_MSG_ERROR([unable to locate librocm_smi64.so and/or rocm_smi.h])
      fi
    fi
  fi
  AM_CONDITIONAL(BUILD_DTK, test "$ac_dtk" = "yes")
])
