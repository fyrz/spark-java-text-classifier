#!/bin/ruby
# -*- encoding : utf-8 -*-

require 'pathname'
require 'set'

# prepare newsgroup files first with:
# recode ISO-8859-1..UTF-8

def readContents(filename)
  file = File.open(filename)
  contents = ""
  file.each { |line|
    contents << line
  }
  contents
end

label_number = 1
File.open("label_index", 'a') do |label_index|
File.open("out", 'a') do |data_file|
  folders = Pathname.new(".").children.select { |c| c.directory? }

  folders.each { |folder|
    label_index.puts "#{label_number},#{folder}"

    files = Dir.glob("#{folder}/*").select{ |e| File.file? e }
    files.each { |f|
      contents = readContents(f).tr!("\n\r", "")
      data_file.puts "#{label_number}|||#{contents}"
    }

    label_number += 1
  }

end
end
