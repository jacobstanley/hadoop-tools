# Don't try fancy stuff like debuginfo, which is useless on binary-only
# packages. Don't strip binary too
# Be sure buildpolicy set to do nothing
%define        __spec_install_post %{nil}
%define          debug_package %{nil}
%define        __os_install_post %{_dbpath}/brp-compress

Summary: Tools for working with Hadoop written with performance in mind.
Name: hadoop-tools
Version: 0.6
Release: 1
License: BSD
Group: Development/Tools
Requires: gmp bash-completion
SOURCE0 : %{name}-%{version}.tar.gz
URL: http://github.com/jystic/hadoop-tools

BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root

%description
%{summary}

%prep
%setup -q

%build
# empty section

%install
%{__rm} -rf %{buildroot}
%{__mkdir} -p  %{buildroot}
# in builddir
%{__cp} -a * %{buildroot}

%clean
%{__rm} -rf %{buildroot}

%files
%defattr(-,root,root,-)
%config %{_sysconfdir}/bash_completion.d/
%{_bindir}/hh

%changelog
* Thu Jul 23 2015 Jacob Stanley <jacob@stanley.io> 0.6-1
- Packaged as RPM
