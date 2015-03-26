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
# Usage:
# $> cd <extracted newsgroup folder>
# $> run script
#
#####################################################################################

require 'pathname'
require 'set'

# Method to read contents of file into a string
def readContents(filename)
  file = File.open(filename)
  contents = ""
  file.each { |line|
    contents << line
  }
  contents
end

label_number = 1

# Open the label index and the data file for writing
File.open("label_index", 'a') do |label_index|
File.open("out", 'a') do |data_file|

  # Retrieve folders in newsgroup folder
  folders = Pathname.new(".").children.select { |c| c.directory? }

  # Traverse through every newsgroup folder
  folders.each { |folder|
    # Add a new entry into the label index
    label_index.puts "#{label_number},#{folder}"

    # Traverse over the newsgroup posts
    files = Dir.glob("#{folder}/*").select{ |e| File.file? e }
    files.each { |f|

      # read every file into a string and remove all line breaks
      contents = readContents(f).tr!("\n\r", "")

      # put the data back into the data out file
      data_file.puts "#{label_number}|||#{contents}"
    }

    # Increase label index counter
    label_number += 1
  }

end
end
