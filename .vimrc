set nocompatible              " be iMproved, required
filetype off                  " required
syntax enable	
set tabstop=4  "number of visual spaces per tab, when file is opened
set softtabstop=4 "number of visual spaces while editing
set expandtab
set number
set showcmd
set cursorline
filetype indent on
set wildmenu "visual autocomplete for command menu
set showmatch " highlight matching [{()}]
set lazyredraw " redraw only when we need to.
set incsearch
set hlsearch
set noswapfile
set guifont=Monospace\ 12
"enable Hive syntaxing
au BufNewFile,BufRead *.hql set filetype=hive expandtab
au BufNewFile,BufRead *.q set filetype=hive expandtab

