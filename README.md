# FlashFind

Welcome to your new gem! In this directory, you'll find the files you need to be able to package up your Ruby library into a gem. Put your Ruby code in the file `lib/flash_find`. To experiment with that code, run `bin/console` for an interactive prompt.

TODO: Delete this and the text above, and describe your gem

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'flash_find'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install flash_find

## Usage

TODO: Write usage instructions here

## Performance

  user     system      total        real
  FlashFind(Maildir)         3.406250   5.210938   8.617188 (  1.645986)
  FlashFind#peach(Maildir)   3.484375   6.960938  10.445313 (  1.918829)
  FastFind(Maildir)          3.375000   2.875000   6.250000 (  3.805505)
  Find(Maildir)              5.000000   5.593750  10.593750 ( 10.586446)
  FlashFind(CVS)            10.148438  14.812500  24.960938 (  2.965290)
  FlashFind#peach(CVS)      10.468750  14.250000  24.718750 (  2.877564)
  FastFind(CVS)              6.554688   8.992188  15.546875 (  2.404545)
  Find(CVS)                  5.937500  10.031250  15.968750 ( 15.335115)
  114233 files in 1829179849 bytes: FlashFind(Maildir), FlashFind#peach(Maildir), FastFind(Maildir), Find(Maildir)
  192151 files in 1669302576 bytes: FlashFind(CVS), FlashFind#peach(CVS), FastFind(CVS), Find(CVS)


## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/flash_find.

