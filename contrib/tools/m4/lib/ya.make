LIBRARY()

VERSION(1.4.17)

LICENSE(
    BSD-3-Clause AND
    GPL-3.0-or-later AND
    LGPL-2.0-or-later
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

IF (NOT MUSL)
    NO_RUNTIME()
ENDIF()

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/tools/m4/lib
)

IF (OS_WINDOWS)
    ADDINCL(
        GLOBAL contrib/tools/m4/lib/platform/win64
    )
ELSE()
    ADDINCL(
        GLOBAL contrib/tools/m4/lib/platform/posix
    )
ENDIF()

IF (OS_DARWIN)
    CFLAGS(
        -D_XOPEN_SOURCE=600
    )
ENDIF()

IF (NOT OS_WINDOWS)
    CFLAGS(
        GLOBAL -Dregcomp=gnu_regcomp
        GLOBAL -Dregerror=gnu_regerror
        GLOBAL -Dregfree=gnu_regfree
        GLOBAL -Dregexec=gnu_regexec
    )
ENDIF()

SRCS(
    asnprintf.c
    basename-lgpl.c
    basename.c
    binary-io.c
    bitrotate.c
    c-ctype.c
    c-stack.c
    c-strcasecmp.c
    c-strncasecmp.c
    clean-temp.c
    cloexec.c
    close-stream.c
    close.c
    closein.c
    closeout.c
    concat-filename.c
    dirname-lgpl.c
    dirname.c
    dup-safer-flag.c
    dup-safer.c
    dup2.c
    error.c
    execute.c
    exitfail.c
    fatal-signal.c
    fclose.c
    fcntl.c
    fd-hook.c
    fd-safer-flag.c
    fd-safer.c
    fflush.c
    filenamecat-lgpl.c
    filenamecat.c
    float.c
    fopen-safer.c
    fpurge.c
    freading.c
    fstat.c
    getdtablesize.c
    getopt.c
    getopt1.c
    gl_avltree_oset.c
    gl_linkedhash_list.c
    gl_list.c
    gl_oset.c
    gl_xlist.c
    gl_xoset.c
    hash.c
    isnand.c
    isnanl.c
    isnanf.c
    itold.c
    localcharset.c
    lseek.c
    malloca.c
    mbrtowc.c
    memchr2.c
    mkstemp-safer.c
    nl_langinfo.c
    pipe-safer.c
    pipe2-safer.c
    printf-args.c
    printf-frexp.c
    printf-frexpl.c
    printf-parse.c
    progname.c
    quotearg.c
    raise.c
    rawmemchr.c
    realloc.c
    regex.c
    rmdir.c
    secure_getenv.c
    sig-handler.c
    signbitd.c
    signbitf.c
    signbitl.c
    spawn-pipe.c
    stat.c
    stpcpy.c
    strdup.c
    stripslash.c
    tempname.c
    tmpdir.c
    unistd.c
    unsetenv.c
    vasnprintf.c
    verror.c
    version-etc-fsf.c
    version-etc.c
    wait-process.c
    wctype-h.c
    xalloc-die.c
    xasprintf.c
    xconcat-filename.c
    xmalloc.c
    xmalloca.c
    xprintf.c
    xsize.c
    xstrndup.c
    xvasprintf.c
)

IF (NOT MUSL)
    SRCS(
        freadahead.c
    )
ENDIF()

IF (NOT OS_LINUX)
    SRCS(
        pipe2.c
    )
ENDIF()

IF (NOT OS_WINDOWS)
    SRCS(
        stdio-write.c
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        wcrtomb.c
        mkstemp.c
        vasprintf.c
        strsignal.c
        mkdtemp.c
        fseeko.c
        fopen.c
        gettimeofday.c
        localeconv.c
        msvc-inval.c
        msvc-nothrow.c
        open.c
        sigaction.c
        sigprocmask.c
        waitpid.c
        wcwidth.c
        uniwidth/width.c
    )
ENDIF()

IF (NOT OS_LINUX OR MUSL)
    SRCS(
        obstack.c
        obstack_printf.c
    )
ENDIF()

IF (OS_CYGWIN OR OS_LINUX)
    #not need it
ELSE()
    SRCS(
        fpending.c
    )
ENDIF()

END()
