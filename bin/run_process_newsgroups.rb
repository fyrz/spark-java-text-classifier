#!/bin/ruby
# -*- encoding : utf-8 -*-

#####################################################################################
#
# Script to turn 20_newsgroups corpus into processable representation
# Reference: http://qwone.com/~jason/20Newsgroups/
#
# Resulting files will be called "labeled_index" and "out"
# - "labeled_index" includes labels with respective ids
# - "out" contains data blocks with id, text pairs separated by three pipes.
#
# Prerequisites:
# Newsgroups files must be converted to UTF-8 using `recode ISO-8859-1..UTF-8`
# or a different mechanism to transform encodings.
#
#####################################################################################

require 'pathname'
require 'set'



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
