# Modseven Database Module

This is the official Database Module for Modseven.

### Why seperate repo?

In Koseven modules are directly in the main repo, for modseven this is no longer necessary, all modules can be included via composer.

### Installation

`composer require modseven/database` ..that's it.

### Configuration

Copy the file(s) from `vendor/modseven/database/conf/` to your `application/conf` folder. Modify them as needed.
Caution: In Koseven the configurations get combined with each other starting from `APPATH` to `SYSPATH` this is *NOT* the case anymore so make sure you copy all contents of the configuration file.

### Usage

Namespace is `\Modseven\Database`, except that it works pretty much like the original one form Kosevevn - [Doku](https://koseven.ga/documentation/database/)
