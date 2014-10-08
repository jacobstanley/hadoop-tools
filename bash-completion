#!/bin/bash
_hh()
{
    local cmdline
    CMDLINE=(--bash-completion-index $COMP_CWORD)

    for arg in ${COMP_WORDS[@]}; do
        CMDLINE=(${CMDLINE[@]} --bash-completion-word $arg)
    done

    COMPREPLY=( $(hh "${CMDLINE[@]}") )
}

complete -o nospace -F _hh hh
