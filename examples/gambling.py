from edgygraph import State, Shared, Node, START, END, Graph
from random import randint
import asyncio

### STATE

class GamblingState(State):

    # Config
    upper: int

    # Runtime
    user_guess: int | None = None
    number: int = 0
    try_count: int = 0
    

### NODES

class GuessNode(Node[GamblingState, Shared]):

    async def __call__(self, state: GamblingState, shared: Shared) -> None:
        
        guess_str = input(f"Guess a number between 1 and {state.upper}: ")

        try:
            state.user_guess = int(guess_str)

            if state.user_guess < 1 or state.user_guess > state.upper:
                print(f"Please enter a number between 1 and {state.upper}.")
                state.user_guess = None

            state.try_count += 1
        except ValueError:
            print("Invalid input. Please enter a number.")
            state.user_guess = None


class RollDiceNode(Node[GamblingState, Shared]):

    async def __call__(self, state: GamblingState, shared: Shared) -> None:
        
        state.number = randint(1, state.upper)


class FailNode(Node[GamblingState, Shared]):

    async def __call__(self, state: GamblingState, shared: Shared) -> None:
        
        print(f"The number is {state.number}. You guessed {state.user_guess}. Try again!")
        state.user_guess = None


class WinNode(Node[GamblingState, Shared]):

    async def __call__(self, state: GamblingState, shared: Shared) -> None:

        if state.try_count == 1:
            print(f"That's amazing! The number is {state.number}. You won with your first try!")

        print(f"The number is {state.number}. You won with {state.try_count} tries!")


### INSTANCES

state = GamblingState(upper=10)
shared = Shared()

guess = GuessNode()
roll = RollDiceNode()
fail = FailNode()
win = WinNode()


### GRAPH

asyncio.run(Graph[GamblingState, Shared](
    edges=[
        (
            START,
            guess,
            lambda st, sh: roll if st.user_guess is not None else guess,

            roll,
            lambda st, sh: win if st.user_guess == st.number else fail,

            fail,
            guess,

            END # join node
        )
    ]
)(state, shared))