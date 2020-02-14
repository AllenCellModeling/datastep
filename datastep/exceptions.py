#!/usr/bin/env python
# -*- coding: utf-8 -*-


class DirectoryNotFoundError(Exception):
    pass


class InvalidGitStatus(Exception):
    pass


class PackagingError(Exception):
    pass
