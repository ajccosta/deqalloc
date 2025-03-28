#include <cstdlib>
#include <iostream>
#include <rapidcheck.h>

int main() {
  rc::check("T", [](unsigned short n) {
    RC_PRE(n > 0);
    auto sum = 0;
    auto expected = 0;
    RC_ASSERT(sum == expected);
  });
}
